package snowflake

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

type recordingReadObserver struct {
	dispatched, acknowledged ReadIdentity
	dispatchErr              error
}

func (o *recordingReadObserver) OnDispatch(_ context.Context, identity ReadIdentity) error {
	o.dispatched = identity
	return o.dispatchErr
}
func (o *recordingReadObserver) OnAcknowledged(_ context.Context, identity ReadIdentity) error {
	o.acknowledged = identity
	return nil
}

func snowflakeMock(t *testing.T) (*DB, sqlmock.Sqlmock) {
	t.Helper()
	raw, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	return &DB{conn: sqlx.NewDb(raw, "sqlmock"), queryIDChannel: func() chan string {
		qid := make(chan string, 1)
		qid <- "01b12345-0000-0000-0000-000000000001"
		close(qid)
		return qid
	}}, mock
}

func TestOpenReadBindsArgumentsAndExposesZeroRowSchema(t *testing.T) {
	t.Parallel()
	db, mock := snowflakeMock(t)
	mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SESSION()").WillReturnRows(sqlmock.NewRows([]string{"account", "database", "session"}).AddRow("ORG.ACCOUNT", "WAREHOUSE", int64(42)))
	mock.ExpectQuery("SELECT amount, payload FROM facts WHERE id = ?").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("amount").OfType("FIXED", &big.Float{}).WithPrecisionAndScale(38, 9),
		sqlmock.NewColumn("payload").OfType("BINARY", []byte{}),
	))
	observer := &recordingReadObserver{}
	stream, identity, err := db.OpenRead(t.Context(), &query.Query{Query: "SELECT amount, payload FROM facts WHERE id = ?", Args: []any{int64(7)}}, "attempt-1", observer)
	require.NoError(t, err)
	require.Equal(t, identity, observer.acknowledged)
	require.Empty(t, observer.dispatched.QueryID)
	require.Equal(t, identity.RequestID, observer.dispatched.RequestID)
	require.Equal(t, "01b12345-0000-0000-0000-000000000001", identity.QueryID)
	require.Len(t, stream.Columns(), 2)
	require.Equal(t, "FIXED", stream.Columns()[0].DatabaseType)
	require.True(t, stream.Columns()[0].DecimalKnown)
	require.False(t, stream.Next())
	require.NoError(t, stream.Err())
	require.NoError(t, stream.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadStreamPreservesAndOwnsHighPrecisionValues(t *testing.T) {
	t.Parallel()
	decimal, _, err := big.ParseFloat("9007199254740993.125", 10, 127, big.ToNearestEven)
	require.NoError(t, err)
	binary := []byte{0, 255}
	got := ownedReadValue(decimal).(*big.Float)
	gotBinary := ownedReadValue(binary).([]byte)
	require.Equal(t, "9007199254740993.125", got.Text('f', 3))
	require.Equal(t, []byte{0, 255}, gotBinary)
	decimal.SetInt64(0)
	binary[0] = 9
	require.Equal(t, "9007199254740993.125", got.Text('f', 3))
	require.Equal(t, []byte{0, 255}, gotBinary)
}

func TestOpenReadJournalsBeforeDispatchAndRejectsUnsafeArguments(t *testing.T) {
	t.Parallel()
	db, mock := snowflakeMock(t)
	mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SESSION()").WillReturnRows(sqlmock.NewRows([]string{"account", "database", "session"}).AddRow("ORG.ACCOUNT", "WAREHOUSE", int64(42)))
	observer := &recordingReadObserver{dispatchErr: errors.New("ledger unavailable")}
	_, _, err := db.OpenRead(t.Context(), &query.Query{Query: "SELECT 1"}, "attempt-1", observer)
	require.ErrorContains(t, err, "dispatch was not accepted")
	require.Empty(t, observer.dispatched.QueryID)
	require.NotEmpty(t, observer.dispatched.RequestID)
	require.NoError(t, mock.ExpectationsWereMet())

	_, _, err = db.OpenRead(t.Context(), &query.Query{Query: "SELECT ?", Args: []any{uint64(7)}}, "attempt-2", &recordingReadObserver{})
	require.ErrorContains(t, err, "argument type is unsupported")
	require.Equal(t, deterministicRequestID("attempt-1"), deterministicRequestID("attempt-1"))
	require.NotEqual(t, deterministicRequestID("attempt-1"), deterministicRequestID("attempt-2"))
}

func TestReadStatusBindsFullIdentityAndCancelRequiresRunningProof(t *testing.T) {
	t.Parallel()
	identity := ReadIdentity{RequestID: deterministicRequestID("attempt-1").String(), QueryID: "qid-1", QueryTag: "cw:attempt-1", Account: "ORG.ACCOUNT", Database: "WAREHOUSE", SessionID: 42}
	db, mock := snowflakeMock(t)
	mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE()").WillReturnRows(sqlmock.NewRows([]string{"account", "database"}).AddRow(identity.Account, identity.Database))
	mock.ExpectQuery(`SELECT EXECUTION_STATUS, COALESCE(QUERY_TAG, '') FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(SESSION_ID => ?, RESULT_LIMIT => 1000)) WHERE QUERY_ID = ?`).WithArgs(identity.SessionID, identity.QueryID).WillReturnRows(sqlmock.NewRows([]string{"status", "tag"}).AddRow("RUNNING", identity.QueryTag))
	state, err := db.ReadStatus(t.Context(), identity)
	require.NoError(t, err)
	require.Equal(t, ReadStateRunning, state)

	mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE()").WillReturnRows(sqlmock.NewRows([]string{"account", "database"}).AddRow(identity.Account, identity.Database))
	mock.ExpectQuery(`SELECT EXECUTION_STATUS, COALESCE(QUERY_TAG, '') FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(SESSION_ID => ?, RESULT_LIMIT => 1000)) WHERE QUERY_ID = ?`).WithArgs(identity.SessionID, identity.QueryID).WillReturnRows(sqlmock.NewRows([]string{"status", "tag"}).AddRow("RUNNING", identity.QueryTag))
	mock.ExpectQuery("SELECT SYSTEM$CANCEL_QUERY(?)").WithArgs(identity.QueryID).WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("query qid-1 cancelled"))
	require.NoError(t, db.CancelRead(t.Context(), identity))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadStatusRejectsMismatchAndUnknownCancellation(t *testing.T) {
	t.Parallel()
	identity := ReadIdentity{RequestID: deterministicRequestID("attempt-1").String(), QueryID: "qid-1", QueryTag: "cw:attempt-1", Account: "ORG.ACCOUNT", Database: "WAREHOUSE", SessionID: 42}
	db, mock := snowflakeMock(t)
	mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE()").WillReturnRows(sqlmock.NewRows([]string{"account", "database"}).AddRow(identity.Account, identity.Database))
	mock.ExpectQuery(`SELECT EXECUTION_STATUS, COALESCE(QUERY_TAG, '') FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(SESSION_ID => ?, RESULT_LIMIT => 1000)) WHERE QUERY_ID = ?`).WithArgs(identity.SessionID, identity.QueryID).WillReturnRows(sqlmock.NewRows([]string{"status", "tag"}).AddRow("RUNNING", "cw:other"))
	state, err := db.ReadStatus(t.Context(), identity)
	require.Error(t, err)
	require.Equal(t, ReadStateIndeterminate, state)

	mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE()").WillReturnRows(sqlmock.NewRows([]string{"account", "database"}).AddRow(identity.Account, identity.Database))
	mock.ExpectQuery(`SELECT EXECUTION_STATUS, COALESCE(QUERY_TAG, '') FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(SESSION_ID => ?, RESULT_LIMIT => 1000)) WHERE QUERY_ID = ?`).WithArgs(identity.SessionID, identity.QueryID).WillReturnRows(sqlmock.NewRows([]string{"status", "tag"}))
	require.ErrorIs(t, db.CancelRead(t.Context(), identity), ErrReadNotActive)
	require.NoError(t, mock.ExpectationsWereMet())
}
