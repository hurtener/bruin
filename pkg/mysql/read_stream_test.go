package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
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
	dispatch     func(context.Context, ReadIdentity) error
	acknowledged func(context.Context, ReadIdentity) error
}

func (o observerFunc) OnDispatch(ctx context.Context, identity ReadIdentity) error {
	return o.dispatch(ctx, identity)
}
func (o observerFunc) OnAcknowledged(ctx context.Context, identity ReadIdentity) error {
	if o.acknowledged != nil {
		return o.acknowledged(ctx, identity)
	}
	return nil
}

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
	mock.ExpectExec("SET SESSION sql_select_limit = 100001").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET @bruin_read_attempt = ?").WithArgs("attempt-1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT CONNECTION_ID(), CURRENT_USER(), COALESCE(DATABASE(), ''), @@server_uuid").WillReturnRows(sqlmock.NewRows([]string{"id", "account", "database", "server_uuid"}).AddRow(42, "reader@%", "warehouse", "server-uuid"))
	mock.ExpectExec("SET SESSION sql_select_limit = ?").WithArgs(100001).WillReturnResult(sqlmock.NewResult(0, 0))
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

func TestOpenReadVerifiedUsesSameTransactionBeforeDispatch(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SET SESSION sql_select_limit = 100001").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET @bruin_read_attempt = ?").WithArgs("attempt-1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT CONNECTION_ID(), CURRENT_USER(), COALESCE(DATABASE(), ''), @@server_uuid").WillReturnRows(sqlmock.NewRows([]string{"id", "account", "database", "server_uuid"}).AddRow(42, "reader@%", "warehouse", "server-uuid"))
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = ?").WithArgs("warehouse").WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("facts"))
	mock.ExpectExec("SET SESSION sql_select_limit = ?").WithArgs(100001).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT amount FROM facts").WillReturnRows(sqlmock.NewRows([]string{"amount"}))
	mock.ExpectRollback()

	client := &Client{readConn: sqlx.NewDb(db, "sqlmock"), control: sqlx.NewDb(db, "sqlmock")}
	observer := &recordingObserver{}
	verify := func(ctx context.Context, session ReadSession) error {
		rows, err := session.Query(ctx, &query.Query{Query: "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = ?", Args: []any{"warehouse"}})
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		values, err := rows.Values()
		require.NoError(t, err)
		require.Equal(t, "facts", values[0])
		return rows.Err()
	}
	stream, _, err := client.OpenReadVerified(t.Context(), &query.Query{Query: "SELECT amount FROM facts"}, "attempt-1", observer, ReadOptions{}, verify)
	require.NoError(t, err)
	require.True(t, observer.acknowledged)
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

func TestCancelReadRejectsChangedServerIdentity(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT @@server_uuid").WillReturnRows(sqlmock.NewRows([]string{"server_uuid"}).AddRow("new-server"))
	client := &Client{readConn: sqlx.NewDb(db, "sqlmock"), control: sqlx.NewDb(db, "sqlmock")}
	err = client.CancelRead(t.Context(), ReadIdentity{ConnectionID: 42, AttemptTag: "attempt-1", Account: "reader@%", Database: "warehouse", ServerUUID: "old-server"}, ReadOptions{})
	require.ErrorIs(t, err, ErrReadNotActive)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadCleanupFailurePreservesRollbackEvidence(t *testing.T) {
	t.Parallel()
	requestErr := context.Canceled
	rollbackErr := errors.New("synthetic rollback failure")
	for _, test := range []struct {
		name     string
		rollback error
		stopped  bool
	}{
		{name: "acknowledged rollback", stopped: true},
		{name: "unobserved concurrent rollback", rollback: sql.ErrTxDone},
		{name: "unresolved rollback", rollback: rollbackErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := readCleanupFailure(requestErr, test.rollback, nil)
			var failure *ReadFailure
			require.ErrorAs(t, err, &failure)
			require.ErrorIs(t, err, requestErr)
			require.Equal(t, test.stopped, failure.Stopped)
			require.Equal(t, test.stopped, failure.ReadStopped())
			if test.rollback != nil && test.rollback != sql.ErrTxDone {
				require.ErrorIs(t, err, test.rollback)
			}
		})
	}
}

func TestAcknowledgedCancellationReturnsExplicitRollbackReceipt(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SET SESSION sql_select_limit = 100001").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET @bruin_read_attempt = ?").WithArgs("attempt-1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT CONNECTION_ID(), CURRENT_USER(), COALESCE(DATABASE(), ''), @@server_uuid").WillReturnRows(sqlmock.NewRows([]string{"id", "account", "database", "server_uuid"}).AddRow(42, "reader@%", "warehouse", "server-uuid"))
	mock.ExpectExec("SET SESSION sql_select_limit = ?").WithArgs(100001).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT amount FROM facts").WillReturnRows(sqlmock.NewRows([]string{"amount"}).AddRow(1).CloseError(context.Canceled)).RowsWillBeClosed()
	mock.ExpectRollback()

	client := &Client{readConn: sqlx.NewDb(db, "sqlmock"), control: sqlx.NewDb(db, "sqlmock")}
	ctx, cancel := context.WithCancel(context.Background())
	observer := observerFunc{
		dispatch: func(context.Context, ReadIdentity) error { return nil },
		acknowledged: func(context.Context, ReadIdentity) error {
			cancel()
			return nil
		},
	}
	stream, _, err := client.OpenRead(ctx, &query.Query{Query: "SELECT amount FROM facts"}, "attempt-1", observer, ReadOptions{})
	require.NoError(t, err)
	closeErr := stream.Close()
	var failure *ReadFailure
	require.ErrorAs(t, closeErr, &failure)
	require.ErrorIs(t, closeErr, context.Canceled)
	require.True(t, failure.Stopped)
	require.True(t, failure.ReadStopped())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenReadCancellationStillBoundsTransactionBegin(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin().WillDelayFor(time.Second)
	client := &Client{readConn: sqlx.NewDb(db, "sqlmock"), control: sqlx.NewDb(db, "sqlmock")}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err = client.OpenRead(ctx, &query.Query{Query: "SELECT 1"}, "attempt-1", &recordingObserver{}, ReadOptions{CancelTimeout: time.Second})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSlowResultCleanupIsBoundedAndRemainsUncertain(t *testing.T) {
	t.Parallel()
	unblock := make(chan struct{})
	stopOnce := sync.Once{}
	stopQuery := func() { stopOnce.Do(func() { close(unblock) }) }
	rollbackErr := errors.New("synthetic rollback was not acknowledged")
	started := time.Now()
	err := finishRead(10*time.Millisecond, stopQuery, func() {}, func() error {
		<-unblock
		return context.Canceled
	}, func() error {
		return rollbackErr
	}, func() error {
		return nil
	})
	var failure *ReadFailure
	require.ErrorAs(t, err, &failure)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, rollbackErr)
	require.False(t, failure.Stopped)
	require.Less(t, time.Since(started), 500*time.Millisecond)
}
