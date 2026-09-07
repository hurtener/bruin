package mssql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/golang-sql/civil"
	mssqldriver "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

var (
	ErrReadIdentity   = errors.New("sqlserver read identity is invalid")
	ErrReadParameter  = errors.New("sqlserver read parameter is unsupported or inexact")
	ErrReadClosed     = errors.New("sqlserver read client is closed")
	ErrReadNotOwned   = errors.New("sqlserver read is not locally owned")
	readTagPattern    = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	readParameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
)

// ReadFailure preserves the query error and separately reports acknowledged
// cleanup of the original owned transaction. It never infers stop from a socket close.
type ReadFailure struct {
	Cause   error
	Stopped bool
}

func (e *ReadFailure) Error() string { return e.Cause.Error() }
func (e *ReadFailure) Unwrap() error { return e.Cause }

type ReadIdentity struct {
	SessionID                             int32
	LoginTime                             time.Time
	Server, Account, Database, AttemptTag string
}

func (i ReadIdentity) valid() bool {
	return i.SessionID > 0 && i.SessionID <= 32767 && !i.LoginTime.IsZero() && i.Server != "" && i.Account != "" && i.Database != "" && readTagPattern.MatchString(i.AttemptTag)
}

type ReadState string

const (
	ReadStateRunning       ReadState = "running"
	ReadStateStopped       ReadState = "stopped"
	ReadStateIndeterminate ReadState = "indeterminate"
)

type ReadOptions struct {
	RequireTLS             bool
	Timeout, CancelTimeout time.Duration
}
type ReadObserver interface {
	OnDispatch(ctx context.Context, identity ReadIdentity) error
	OnAcknowledged(ctx context.Context, identity ReadIdentity) error
}

// ReadSession permits native metadata checks on the same connection and
// repeatable-read transaction as execution. SQL Server has no read-only
// transaction option; the caller must supply a SELECT-only database credential.
type ReadSession interface {
	Query(ctx context.Context, q *query.Query) (query.RowStream, error)
}
type (
	ReadVerifier func(context.Context, ReadSession) error
	activeRead   struct {
		identity    ReadIdentity
		cancel      context.CancelFunc
		ready, done chan struct{}
		finish      func() error
	}
)

func (db *DB) initializeReadPools(o ReadOptions) error {
	db.readMu.Lock()
	defer db.readMu.Unlock()
	if db.readClosed {
		return ErrReadClosed
	}
	if db.config == nil {
		return ErrReadIdentity
	}
	parsed, err := msdsn.Parse(db.config.ToDBConnectionURI())
	if err != nil {
		return ErrReadIdentity
	}
	if o.RequireTLS && (parsed.TLSConfig == nil || parsed.TLSConfig.InsecureSkipVerify || (parsed.Encryption != msdsn.EncryptionRequired && parsed.Encryption != msdsn.EncryptionStrict)) {
		return ErrReadIdentity
	}
	if db.readDB != nil {
		return nil
	}
	// The sqlserver driver preserves native @name/@pN markers. The legacy mssql
	// registration preprocesses SQL and is intentionally not used by this leaf.
	readDB, err := sql.Open("sqlserver", db.config.ToDBConnectionURI())
	if err != nil {
		return err
	}
	control, err := sql.Open("sqlserver", db.config.ToDBConnectionURI())
	if err != nil {
		_ = readDB.Close()
		return err
	}
	readDB.SetMaxOpenConns(8)
	readDB.SetMaxIdleConns(0)
	control.SetMaxOpenConns(2)
	control.SetMaxIdleConns(2)
	db.readDB, db.readControl = readDB, control
	db.activeReads = make(map[string]*activeRead)
	return nil
}

func (db *DB) Close() error {
	db.readMu.Lock()
	if db.readClosed {
		db.readMu.Unlock()
		return nil
	}
	db.readClosed = true
	active := make([]*activeRead, 0, len(db.activeReads))
	for _, a := range db.activeReads {
		active = append(active, a)
	}
	readDB, control, original := db.readDB, db.readControl, db.conn
	db.readMu.Unlock()
	for _, a := range active {
		a.cancel()
	}
	var result error
	for _, a := range active {
		<-a.ready
		result = errors.Join(result, a.finish())
	}
	if readDB != nil {
		result = errors.Join(result, readDB.Close())
	}
	if control != nil {
		result = errors.Join(result, control.Close())
	}
	if original != nil {
		result = errors.Join(result, original.Close())
	}
	return result
}

const (
	readIdentitySQL = `SELECT @@SPID, login_time, CAST(SERVERPROPERTY('ServerName') AS nvarchar(128)), ORIGINAL_LOGIN(), DB_NAME() FROM sys.dm_exec_sessions WHERE session_id=@@SPID`
	readTagSQL      = `SET CONTEXT_INFO @p1`
)

func (db *DB) OpenRead(ctx context.Context, q *query.Query, attempt string, observer ReadObserver, o ReadOptions) (*ReadStream, ReadIdentity, error) {
	return db.OpenReadVerified(ctx, q, attempt, observer, o, nil)
}

func (db *DB) OpenReadVerified(ctx context.Context, q *query.Query, attempt string, observer ReadObserver, o ReadOptions, verify ReadVerifier) (*ReadStream, ReadIdentity, error) {
	if ctx == nil || q == nil || observer == nil || !readTagPattern.MatchString(attempt) || strings.TrimSpace(q.Query) == "" || len(q.VariableDefinitions) != 0 || o.Timeout < time.Millisecond || o.Timeout > time.Hour {
		return nil, ReadIdentity{}, ErrReadParameter
	}
	args, err := readArguments(q.Args)
	if err != nil {
		return nil, ReadIdentity{}, err
	}
	if err = db.initializeReadPools(o); err != nil {
		return nil, ReadIdentity{}, err
	}
	work, cancel := context.WithTimeout(ctx, o.Timeout)
	db.readMu.Lock()
	pool := db.readDB
	closed := db.readClosed
	db.readMu.Unlock()
	if closed {
		cancel()
		return nil, ReadIdentity{}, ErrReadClosed
	}
	conn, err := pool.Conn(work)
	if err != nil {
		cancel()
		return nil, ReadIdentity{}, err
	}
	discard := func() { _ = conn.Raw(func(any) error { return driver.ErrBadConn }); _ = conn.Close() }
	// Query I/O keeps work's deadline. Only transaction cleanup receives a
	// separate bounded allowance, so cancellation does not hide an automatic
	// rollback result before the owner can observe its acknowledgement.
	grace := o.CancelTimeout
	if grace < time.Millisecond || grace > 30*time.Second {
		grace = time.Second
	}
	until, _ := work.Deadline()
	cleanupContext, stopTransaction := context.WithDeadline(context.WithoutCancel(work), until.Add(grace))
	tx, err := conn.BeginTx(cleanupContext, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		stopTransaction()
		cancel()
		discard()
		return nil, ReadIdentity{}, err
	}
	identity := ReadIdentity{AttemptTag: attempt}
	err = tx.QueryRowContext(work, readIdentitySQL).Scan(&identity.SessionID, &identity.LoginTime, &identity.Server, &identity.Account, &identity.Database)
	if err != nil || !identity.valid() {
		cancel()
		_ = tx.Rollback()
		stopTransaction()
		discard()
		if err == nil {
			err = ErrReadIdentity
		}
		return nil, identity, err
	}
	if _, err = tx.ExecContext(work, readTagSQL, []byte(attempt)); err != nil {
		cancel()
		_ = tx.Rollback()
		stopTransaction()
		discard()
		return nil, identity, err
	}
	a := &activeRead{identity: identity, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}
	var rows *sql.Rows
	var finishOnce sync.Once
	var finishErr error
	a.finish = func() error {
		finishOnce.Do(func() {
			cancel()
			if rows != nil {
				finishErr = rows.Close()
			}
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				finishErr = errors.Join(finishErr, rollbackErr)
			}
			stopTransaction()
			discard()
			db.readMu.Lock()
			if db.activeReads[attempt] == a {
				delete(db.activeReads, attempt)
			}
			db.readMu.Unlock()
			close(a.done)
		})
		return finishErr
	}
	db.readMu.Lock()
	if db.readClosed || db.activeReads[attempt] != nil {
		db.readMu.Unlock()
		_ = a.finish()
		return nil, identity, ErrReadNotOwned
	}
	db.activeReads[attempt] = a
	db.readMu.Unlock()
	defer close(a.ready)
	fail := func(e error) (*ReadStream, ReadIdentity, error) {
		cleanupErr := a.finish()
		return nil, identity, &ReadFailure{Cause: errors.Join(e, cleanupErr), Stopped: cleanupErr == nil}
	}
	if verify != nil {
		if err = verify(work, readSession{tx: tx}); err != nil {
			return fail(err)
		}
	}
	if err = observer.OnDispatch(work, identity); err != nil {
		return fail(err)
	}
	rows, err = tx.QueryContext(work, q.String(), args...)
	if err != nil {
		return fail(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return fail(err)
	}
	columns := query.ColumnsFromSQL(types)
	if len(columns) == 0 {
		return fail(ErrReadParameter)
	}
	if err = observer.OnAcknowledged(work, identity); err != nil {
		return fail(err)
	}
	if err = work.Err(); err != nil {
		return fail(err)
	}
	return &ReadStream{rows: rows, columns: columns, cancel: cancel, finish: a.finish}, identity, nil
}

// CancelRead cancels only the original locally reserved connection. A new client
// cannot reconstruct that ownership from a reusable SPID, so restart cancellation
// returns indeterminate instead of issuing KILL.
func (db *DB) CancelRead(ctx context.Context, id ReadIdentity, o ReadOptions) (ReadState, error) {
	if ctx == nil || !id.valid() || o.CancelTimeout < time.Millisecond || o.CancelTimeout > 30*time.Second {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	db.readMu.Lock()
	a := db.activeReads[id.AttemptTag]
	db.readMu.Unlock()
	if a == nil || a.identity != id {
		return ReadStateIndeterminate, ErrReadNotOwned
	}
	a.cancel()
	bounded, cancel := context.WithTimeout(ctx, o.CancelTimeout)
	defer cancel()
	select {
	case <-a.ready:
	case <-bounded.Done():
		return ReadStateIndeterminate, bounded.Err()
	}
	// QueryContext and driver attention have returned before ready closes. Closing
	// the rows/transaction joins native cleanup on that exact connection.
	if err := a.finish(); err != nil {
		return ReadStateIndeterminate, err
	}
	return ReadStateStopped, nil
}

const readStatusSQL = `SELECT COUNT(*), COALESCE(MAX(CASE WHEN status='sleeping' THEN 0 ELSE 1 END),0) FROM sys.dm_exec_sessions WHERE session_id=@p1 AND login_time=@p2 AND original_login_name=@p3 AND database_id=DB_ID(@p4) AND context_info=@p5`

// ReadStatus requires visibility of the original session in the DMV. Absence or
// unavailable visibility is indeterminate. Server name is not a boot epoch.
func (db *DB) ReadStatus(ctx context.Context, id ReadIdentity, o ReadOptions) (ReadState, error) {
	if ctx == nil || !id.valid() {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	if err := db.initializeReadPools(o); err != nil {
		return ReadStateIndeterminate, err
	}
	db.readMu.Lock()
	control := db.readControl
	db.readMu.Unlock()
	conn, err := control.Conn(ctx)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	defer conn.Close()
	var server string
	if err = conn.QueryRowContext(ctx, `SELECT CAST(SERVERPROPERTY('ServerName') AS nvarchar(128))`).Scan(&server); err != nil {
		return ReadStateIndeterminate, err
	}
	if server != id.Server {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	var count, running int
	err = conn.QueryRowContext(ctx, readStatusSQL, id.SessionID, mssqldriver.DateTime1(id.LoginTime), id.Account, id.Database, []byte(id.AttemptTag)).Scan(&count, &running)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	if count != 1 {
		return ReadStateIndeterminate, nil
	}
	if running == 0 {
		return ReadStateStopped, nil
	}
	return ReadStateRunning, nil
}

type readSession struct{ tx *sql.Tx }

func (s readSession) Query(ctx context.Context, q *query.Query) (query.RowStream, error) {
	if q == nil || len(q.VariableDefinitions) != 0 {
		return nil, ErrReadParameter
	}
	args, err := readArguments(q.Args)
	if err != nil {
		return nil, err
	}
	rows, err := s.tx.QueryContext(ctx, q.String(), args...)
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	return &ReadStream{rows: rows, columns: query.ColumnsFromSQL(types), finish: rows.Close}, nil
}

func readArguments(args []any) ([]any, error) {
	out := make([]any, len(args))
	names := map[string]bool{}
	for i, value := range args {
		name := ""
		if named, ok := value.(sql.NamedArg); ok {
			name = named.Name
			value = named.Value
			if !readParameterName.MatchString(name) || names[strings.ToLower(name)] {
				return nil, ErrReadParameter
			}
			names[strings.ToLower(name)] = true
		}
		copied, err := readArgument(value)
		if err != nil {
			return nil, err
		}
		if name != "" {
			out[i] = sql.Named(name, copied)
		} else {
			out[i] = copied
		}
	}
	return out, nil
}

func readArgument(v any) (any, error) {
	switch value := v.(type) {
	case nil, bool, int, int16, int32, int64, uint8:
		return value, nil
	case string:
		if utf8.ValidString(value) {
			return value, nil
		}
	case []byte:
		return append([]byte(nil), value...), nil
	case float64:
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			return value, nil
		}
	case float32:
		if !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0) {
			return value, nil
		}
	case time.Time:
		_, offset := value.Zone()
		if value.Year() >= 1 && value.Year() <= 9999 && value.UTC().Year() >= 1 && value.UTC().Year() <= 9999 && value.Nanosecond()%100 == 0 && offset%60 == 0 && offset >= -14*3600 && offset <= 14*3600 {
			return mssqldriver.DateTimeOffset(value), nil
		}
	case civil.Date:
		if value.IsValid() && value.Year >= 1 && value.Year <= 9999 {
			return value, nil
		}
	case civil.Time:
		if value.IsValid() && value.Nanosecond%100 == 0 {
			return value, nil
		}
	case civil.DateTime:
		if value.IsValid() && value.Date.Year >= 1 && value.Date.Year <= 9999 && value.Time.Nanosecond%100 == 0 {
			return value, nil
		}
	}
	// SDK1.8.2 has no native decimal input encoder. Callers may explicitly bind
	// lexical strings in already validated CAST SQL; no decimal-to-float fallback.
	return nil, ErrReadParameter
}

type ReadStream struct {
	rows    *sql.Rows
	columns []query.Column
	cancel  context.CancelFunc
	finish  func() error
	mu      sync.Mutex
	closed  bool
}

var _ query.RowStream = (*ReadStream)(nil)

func (s *ReadStream) Columns() []query.Column { return append([]query.Column(nil), s.columns...) }
func (s *ReadStream) Next() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	return s.rows.Next()
}

func (s *ReadStream) Values() ([]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrReadClosed
	}
	values := make([]any, len(s.columns))
	targets := make([]any, len(values))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := s.rows.Scan(targets...); err != nil {
		return nil, err
	}
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = append([]byte(nil), b...)
		}
	}
	return values, nil
}
func (s *ReadStream) Err() error { return s.rows.Err() }
func (s *ReadStream) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.finish()
}
