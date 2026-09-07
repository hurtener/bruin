package mysql

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	dispatched   bool
	acknowledged bool
}

type testConfig string

func (c testConfig) GetIngestrURI() string     { return string(c) }
func (c testConfig) ToDBConnectionURI() string { return string(c) }

func TestOpenReadMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("BRUIN_MYSQL_READ_TEST_DSN")
	if dsn == "" {
		t.Skip("BRUIN_MYSQL_READ_TEST_DSN is not set")
	}
	client, err := NewClient(testConfig(dsn))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	observer := &recordingObserver{}
	stream, _, err := client.OpenRead(t.Context(), &query.Query{Query: "SELECT CAST('9007199254740993.125' AS DECIMAL(30,3)) AS amount, CAST(? AS SIGNED) AS bound_value, CAST(X'00FF' AS BINARY(2)) AS payload", Args: []any{int64(9007199254740993)}}, "exact-types-1", observer, ReadOptions{})
	require.NoError(t, err)
	require.True(t, stream.Next())
	values, err := stream.Values()
	require.NoError(t, err)
	require.Equal(t, []byte("9007199254740993.125"), values[0])
	require.Equal(t, int64(9007199254740993), values[1])
	require.Equal(t, []byte{0, 255}, values[2])
	require.NoError(t, stream.Close())

	dispatched := make(chan ReadIdentity, 1)
	cancelObserver := observerFunc{dispatch: func(_ context.Context, identity ReadIdentity) error { dispatched <- identity; return nil }}
	result := make(chan error, 1)
	go func() {
		stream, _, err := client.OpenRead(context.Background(), &query.Query{Query: "SELECT SLEEP(5) /* bruin_cancel_probe */"}, "cancel-1", cancelObserver, ReadOptions{})
		if err == nil {
			for stream.Next() {
				_, err = stream.Values()
			}
			if err == nil {
				err = stream.Err()
			}
			closeErr := stream.Close()
			if err == nil {
				err = closeErr
			}
		}
		result <- err
	}()
	identity := <-dispatched
	started := time.Now()
	time.Sleep(100 * time.Millisecond)
	restartedClient, err := NewClient(testConfig(dsn))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restartedClient.Close()) })
	state, err := restartedClient.ReadStatus(t.Context(), identity, ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, ReadStateRunning, state)

	wrongTag := identity
	wrongTag.AttemptTag = "cancel-other"
	state, err = restartedClient.ReadStatus(t.Context(), wrongTag, ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, ReadStateIndeterminate, state)
	wrongAccount := identity
	wrongAccount.Account = "other@%"
	state, err = restartedClient.ReadStatus(t.Context(), wrongAccount, ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, ReadStateIndeterminate, state)
	wrongDatabase := identity
	wrongDatabase.Database = "other"
	state, err = restartedClient.ReadStatus(t.Context(), wrongDatabase, ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, ReadStateIndeterminate, state)

	require.NoError(t, restartedClient.CancelRead(t.Context(), identity, ReadOptions{}))
	select {
	case <-result:
		require.Less(t, time.Since(started), 2*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled MySQL query did not return promptly")
	}
	var remaining int
	require.NoError(t, restartedClient.control.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.processlist WHERE ID = ? AND INFO LIKE '%bruin_cancel_probe%'", identity.ConnectionID).Scan(&remaining))
	require.Zero(t, remaining, "cancelled query is still executing on the server")
}

type observerFunc struct {
	dispatch func(context.Context, ReadIdentity) error
}

func (o observerFunc) OnDispatch(ctx context.Context, identity ReadIdentity) error {
	return o.dispatch(ctx, identity)
}
func (o observerFunc) OnAcknowledged(context.Context, ReadIdentity) error { return nil }

func (o *recordingObserver) OnDispatch(_ context.Context, identity ReadIdentity) error {
	o.dispatched = identity.ConnectionID == 42 && identity.AttemptTag == "attempt-1"
	return nil
}

func (o *recordingObserver) OnAcknowledged(_ context.Context, identity ReadIdentity) error {
	o.acknowledged = o.dispatched && identity.ConnectionID == 42
	return nil
}

func TestOpenReadBindsArgumentsAndExposesEmptySchema(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SET @bruin_read_attempt = ?").WithArgs("attempt-1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT CONNECTION_ID(), CURRENT_USER(), COALESCE(DATABASE(), '')").WillReturnRows(sqlmock.NewRows([]string{"id", "account", "database"}).AddRow(42, "reader@%", "warehouse"))
	mock.ExpectQuery("SELECT amount, payload FROM facts WHERE id = ?").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("amount").OfType("DECIMAL", []byte{}).WithPrecisionAndScale(38, 9),
		sqlmock.NewColumn("payload").OfType("BLOB", []byte{}),
	))
	mock.ExpectRollback()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	client := &Client{readConn: sqlxDB, control: sqlxDB}
	observer := &recordingObserver{}
	stream, identity, err := client.OpenRead(t.Context(), &query.Query{Query: "SELECT amount, payload FROM facts WHERE id = ?", Args: []any{int64(7)}}, "attempt-1", observer, ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(42), identity.ConnectionID)
	require.True(t, observer.acknowledged)
	columns := stream.Columns()
	require.Len(t, columns, 2)
	require.Equal(t, "DECIMAL", columns[0].DatabaseType)
	require.True(t, columns[0].DecimalKnown)
	require.False(t, stream.Next())
	require.NoError(t, stream.Err())
	require.NoError(t, stream.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenReadRejectsMissingTLSBeforeConnecting(t *testing.T) {
	t.Parallel()
	client, err := NewClient(Config{Username: "reader", Password: "secret", Host: "127.0.0.1", Database: "warehouse"})
	require.NoError(t, err)
	_, _, err = client.OpenRead(t.Context(), &query.Query{Query: "SELECT 1"}, "attempt-1", &recordingObserver{}, ReadOptions{RequireTLS: true})
	require.EqualError(t, err, "mysql governed read requires explicit TLS")
}

func TestGovernedReadDSNDisablesMultiStatementsAndInterpolation(t *testing.T) {
	t.Parallel()
	dsn, err := governedReadDSN("reader:secret@tcp(localhost:3306)/warehouse?multiStatements=true&interpolateParams=true&tls=true", true)
	require.NoError(t, err)
	parsed, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	require.False(t, parsed.MultiStatements)
	require.False(t, parsed.InterpolateParams)
	require.Equal(t, "true", parsed.TLSConfig)
}
