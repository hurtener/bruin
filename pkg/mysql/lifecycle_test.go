package mysql

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

type lifecycleConfig struct{ calls atomic.Int32 }

func (*lifecycleConfig) GetIngestrURI() string { return "unused" }
func (c *lifecycleConfig) ToDBConnectionURI() string {
	c.calls.Add(1)
	return "invalid synthetic DSN"
}

func lifecycleCalls(c *Client) map[string]func(context.Context) error {
	q := &query.Query{Query: "SELECT 1"}
	identity := ReadIdentity{ConnectionID: 42, AttemptTag: "attempt-1", Account: "reader@%", Database: "warehouse", ServerUUID: "server-uuid"}
	return map[string]func(context.Context) error{
		"execute": func(ctx context.Context) error { return c.RunQueryWithoutResult(ctx, q) },
		"select":  func(ctx context.Context) error { _, err := c.Select(ctx, q); return err },
		"schema":  func(ctx context.Context) error { _, err := c.SelectWithSchema(ctx, q); return err },
		"open_read": func(ctx context.Context) error {
			stream, _, err := c.OpenRead(ctx, q, "attempt-1", &recordingObserver{}, ReadOptions{})
			if stream != nil {
				_ = stream.Close()
			}
			return err
		},
		"status": func(ctx context.Context) error { _, err := c.ReadStatus(ctx, identity, ReadOptions{}); return err },
		"cancel": func(ctx context.Context) error { return c.CancelRead(ctx, identity, ReadOptions{}) },
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
			config := &lifecycleConfig{}
			client := &Client{config: config}
			if initialized {
				raw, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectClose()
				pool := sqlx.NewDb(raw, "sqlmock")
				client.conn, client.readConn, client.control = pool, pool, pool
				t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })
			}
			require.NoError(t, client.Close())
			for name, call := range lifecycleCalls(client) {
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					require.ErrorIs(t, call(t.Context()), ErrClientClosed)
					require.NoError(t, client.Close())
					require.Zero(t, config.calls.Load(), "retirement must not resolve old credentials")
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
	client := &Client{conn: pool, readConn: pool, control: pool}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	calls := lifecycleCalls(client)
	results := make(chan error, len(calls))
	for _, call := range calls {
		go func() { results <- call(ctx) }()
	}
	require.Eventually(t, func() bool { return pool.Stats().WaitCount == int64(len(calls)) }, time.Second, time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
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
			raw, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			pool := sqlx.NewDb(raw, "sqlmock")
			t.Cleanup(func() { _ = pool.Close() })
			mock.ExpectBegin()
			mock.ExpectExec("SET SESSION sql_select_limit = 100001").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("SET @bruin_read_attempt = ?").WithArgs("attempt-1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("SELECT CONNECTION_ID(), CURRENT_USER(), COALESCE(DATABASE(), ''), @@server_uuid").WillReturnRows(sqlmock.NewRows([]string{"id", "account", "database", "server_uuid"}).AddRow(42, "reader@%", "warehouse", "server-uuid"))
			mock.ExpectExec("SET SESSION sql_select_limit = ?").WithArgs(100001).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(int64(1))).RowsWillBeClosed()
			mock.ExpectRollback()
			mock.ExpectClose()
			client := &Client{readConn: pool, control: pool}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream, _, err := client.OpenRead(ctx, &query.Query{Query: "SELECT 1"}, "attempt-1", &recordingObserver{}, ReadOptions{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = stream.Close() })
			require.NoError(t, client.Close())
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
				if cancelRead && err != nil {
					require.ErrorIs(t, err, sql.ErrConnDone)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("retired client's captured read session could not close")
			}
			require.Eventually(t, func() bool { return pool.Stats().OpenConnections == 0 }, time.Second, time.Millisecond)
			client.readMutex.Lock()
			active := len(client.activeReads)
			client.readMutex.Unlock()
			require.Zero(t, active)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
