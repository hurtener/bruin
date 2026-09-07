package snowflake

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func lifecycleCalls(db *DB) map[string]func(context.Context) error {
	q := &query.Query{Query: "SELECT 1"}
	identity := ReadIdentity{RequestID: deterministicRequestID("attempt-1", "ORG.ACCOUNT", "WAREHOUSE").String(), QueryID: "qid-1", QueryTag: "cw:attempt-1", Account: "ORG.ACCOUNT", Database: "WAREHOUSE", SessionID: 42}
	return map[string]func(context.Context) error{
		"execute":     func(ctx context.Context) error { return db.RunQueryWithoutResult(ctx, q) },
		"select":      func(ctx context.Context) error { _, err := db.Select(ctx, q); return err },
		"last_result": func(ctx context.Context) error { _, err := db.SelectOnlyLastResult(ctx, q); return err },
		"validate":    func(ctx context.Context) error { _, err := db.IsValid(ctx, q); return err },
		"schema":      func(ctx context.Context) error { _, err := db.SelectWithSchema(ctx, q); return err },
		"open_read": func(ctx context.Context) error {
			stream, _, err := db.OpenRead(ctx, q, "attempt-1", &recordingReadObserver{})
			if stream != nil {
				_ = stream.Close()
			}
			return err
		},
		"status": func(ctx context.Context) error { _, err := db.ReadStatus(ctx, identity); return err },
		"cancel": func(ctx context.Context) error { return db.CancelRead(ctx, identity) },
	}
}

func TestClosePermanentlyRetiresClient(t *testing.T) {
	t.Parallel()
	for _, initialized := range []bool{false, true} {
		name := "before_initialization"
		if initialized {
			name = "after_initialization"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var connects atomic.Int32
			db := &DB{connect: func(context.Context) (*sqlx.DB, error) {
				connects.Add(1)
				return nil, errors.New("unexpected connection attempt with retired credentials")
			}}
			if initialized {
				raw, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectClose()
				db.conn = sqlx.NewDb(raw, "sqlmock")
				t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })
			}
			require.NoError(t, db.Close())
			for name, call := range lifecycleCalls(db) {
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					require.ErrorIs(t, call(t.Context()), ErrClientClosed)
					require.NoError(t, db.Close())
					require.Zero(t, connects.Load(), "retirement must not reopen old credentials")
				})
			}
		})
	}
}

func TestCloseUnblocksPendingPoolAcquisitions(t *testing.T) {
	t.Parallel()
	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	pool := sqlx.NewDb(raw, "sqlmock")
	pool.SetMaxOpenConns(1)
	held, err := pool.Connx(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close(); _ = pool.Close() })
	mock.ExpectClose()
	db := &DB{conn: pool}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	calls := lifecycleCalls(db)
	results := make(chan error, len(calls))
	for _, call := range calls {
		go func() { results <- call(ctx) }()
	}
	require.Eventually(t, func() bool { return pool.Stats().WaitCount == int64(len(calls)) }, time.Second, time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("retirement waited on pending acquisitions")
	}
	for range calls {
		select {
		case err := <-results:
			require.ErrorContains(t, err, "database is closed")
		case <-time.After(time.Second):
			t.Fatal("pool acquisition remained blocked after retirement")
		}
	}
	require.NoError(t, held.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestActiveReadCanFinishAfterClientRetirement(t *testing.T) {
	t.Parallel()
	for _, cancelRead := range []bool{false, true} {
		name := "close"
		if cancelRead {
			name = "caller_cancel"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, mock := snowflakeMock(t)
			pool := db.conn
			mock.ExpectQuery("SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SESSION()").WillReturnRows(sqlmock.NewRows([]string{"account", "database", "session"}).AddRow("ORG.ACCOUNT", "WAREHOUSE", int64(42)))
			mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(int64(1))).RowsWillBeClosed()
			mock.ExpectClose()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream, _, err := db.OpenRead(ctx, &query.Query{Query: "SELECT 1"}, "attempt-1", &recordingReadObserver{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = stream.Close() })
			require.NoError(t, db.Close())
			require.True(t, stream.Next(), "already owned rows remain readable")
			values, err := stream.Values()
			require.NoError(t, err)
			require.Equal(t, []any{int64(1)}, values)
			if cancelRead {
				cancel()
			}
			finished := make(chan error, 1)
			go func() { finished <- stream.Close() }()
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("retired client's captured read session could not close")
			}
			require.Zero(t, pool.Stats().OpenConnections)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
