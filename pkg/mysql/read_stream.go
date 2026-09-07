package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/bruin-data/bruin/pkg/query"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	ErrReadNotActive = errors.New("mysql read is not active")
	ErrReadIdentity  = errors.New("mysql read identity is invalid")
	readTagPattern   = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

type ReadState string

const (
	ReadStateRunning         ReadState = "running"
	ReadStateCancelRequested ReadState = "cancel_requested"
	ReadStateIndeterminate   ReadState = "indeterminate"
)

// ReadIdentity is durable native evidence for one MySQL read.
type ReadIdentity struct {
	ConnectionID uint64
	AttemptTag   string
	Account      string
	Database     string
}

func (i ReadIdentity) valid() bool {
	return i.ConnectionID != 0 && readTagPattern.MatchString(i.AttemptTag) && i.Account != ""
}

// ReadObserver journals dispatch before user SQL and acknowledges only after
// MySQL has accepted the query and exposed its result metadata.
type ReadObserver interface {
	OnDispatch(context.Context, ReadIdentity) error
	OnAcknowledged(context.Context, ReadIdentity) error
}

type ReadOptions struct {
	// RequireTLS rejects a native DSN that has no explicit TLS mode. Named TLS
	// configurations must already be registered with go-sql-driver/mysql.
	// Restart reconciliation additionally requires SELECT on only
	// performance_schema.threads and performance_schema.user_variables_by_thread;
	// without both, ReadStatus returns indeterminate with the native error.
	RequireTLS bool
}

type activeRead struct {
	identity ReadIdentity
	state    ReadState
}

// ReadStatus checks durable session evidence. Absence is indeterminate because
// it cannot distinguish completion from lost server state.
func (c *Client) ReadStatus(ctx context.Context, identity ReadIdentity, options ReadOptions) (ReadState, error) {
	if !identity.valid() {
		return "", ErrReadIdentity
	}
	if err := c.initializeReadDB(ctx, options.RequireTLS); err != nil {
		return "", err
	}
	var count int
	err := c.control.QueryRowContext(ctx, `SELECT COUNT(*) FROM performance_schema.threads t JOIN performance_schema.user_variables_by_thread v ON v.THREAD_ID=t.THREAD_ID WHERE t.PROCESSLIST_ID=? AND t.PROCESSLIST_USER=SUBSTRING_INDEX(?, '@', 1) AND COALESCE(t.PROCESSLIST_DB, '')=? AND v.VARIABLE_NAME='bruin_read_attempt' AND CAST(v.VARIABLE_VALUE AS CHAR)=?`, identity.ConnectionID, identity.Account, identity.Database, identity.AttemptTag).Scan(&count)
	if err != nil {
		return ReadStateIndeterminate, fmt.Errorf("failed to reconcile mysql read identity: %w", err)
	}
	if count == 0 {
		return ReadStateIndeterminate, nil
	}
	c.readMutex.Lock()
	defer c.readMutex.Unlock()
	if active, ok := c.activeReads[identity.AttemptTag]; ok && active.identity == identity {
		return active.state, nil
	}
	return ReadStateRunning, nil
}

// CancelRead interrupts only a session whose durable tag/account/database
// evidence still matches the retained identity.
func (c *Client) CancelRead(ctx context.Context, identity ReadIdentity, options ReadOptions) error {
	if !identity.valid() {
		return ErrReadIdentity
	}
	c.readMutex.Lock()
	active, locallyOwned := c.activeReads[identity.AttemptTag]
	if locallyOwned && active.identity == identity {
		active.state = ReadStateCancelRequested
	} else {
		locallyOwned = false
	}
	c.readMutex.Unlock()
	if !locallyOwned {
		state, err := c.ReadStatus(ctx, identity, options)
		if err != nil {
			return err
		}
		if state == ReadStateIndeterminate {
			return ErrReadNotActive
		}
	}
	// KILL QUERY has no parameter marker. This value was decoded as uint64.
	if _, err := c.control.ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", identity.ConnectionID)); err != nil {
		return fmt.Errorf("failed to cancel mysql read: %w", err)
	}
	return nil
}

// OpenRead reserves one read-only session, publishes its identity, and only
// then dispatches the caller's typed query and arguments.
func (c *Client) OpenRead(ctx context.Context, queryObj *query.Query, attemptTag string, observer ReadObserver, options ReadOptions) (*ReadStream, ReadIdentity, error) {
	if queryObj == nil || observer == nil {
		return nil, ReadIdentity{}, errors.New("query and read observer are required")
	}
	if !readTagPattern.MatchString(attemptTag) {
		return nil, ReadIdentity{}, ErrReadIdentity
	}
	if err := c.initializeReadDB(ctx, options.RequireTLS); err != nil {
		return nil, ReadIdentity{}, err
	}
	conn, err := c.readConn.Connx(ctx)
	if err != nil {
		return nil, ReadIdentity{}, fmt.Errorf("failed to reserve mysql connection: %w", err)
	}
	fail := func(err error) (*ReadStream, ReadIdentity, error) { _ = conn.Close(); return nil, ReadIdentity{}, err }
	tx, err := conn.BeginTxx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fail(fmt.Errorf("failed to begin mysql read-only transaction: %w", err))
	}
	var identity ReadIdentity
	if _, err := tx.ExecContext(ctx, "SET @bruin_read_attempt = ?", attemptTag); err != nil {
		_ = tx.Rollback()
		return fail(fmt.Errorf("failed to tag mysql read session: %w", err))
	}
	if err := tx.QueryRowContext(ctx, "SELECT CONNECTION_ID(), CURRENT_USER(), COALESCE(DATABASE(), '')").Scan(&identity.ConnectionID, &identity.Account, &identity.Database); err != nil {
		_ = tx.Rollback()
		return fail(fmt.Errorf("failed to identify mysql read session: %w", err))
	}
	identity.AttemptTag = attemptTag
	c.readMutex.Lock()
	if c.activeReads == nil {
		c.activeReads = make(map[string]*activeRead)
	}
	if _, exists := c.activeReads[attemptTag]; exists {
		c.readMutex.Unlock()
		_ = tx.Rollback()
		return fail(errors.New("mysql read attempt tag is already active"))
	}
	c.activeReads[attemptTag] = &activeRead{identity: identity, state: ReadStateRunning}
	c.readMutex.Unlock()
	if err := observer.OnDispatch(ctx, identity); err != nil {
		c.removeActiveRead(identity)
		_ = tx.Rollback()
		return fail(fmt.Errorf("mysql read dispatch was not accepted: %w", err))
	}
	rows, err := tx.QueryContext(ctx, queryObj.String(), queryObj.Args...)
	if err != nil {
		c.removeActiveRead(identity)
		_ = tx.Rollback()
		return fail(fmt.Errorf("failed to execute mysql read: %w", err))
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		c.removeActiveRead(identity)
		_ = tx.Rollback()
		return fail(fmt.Errorf("failed to retrieve mysql result schema: %w", err))
	}
	if err := observer.OnAcknowledged(ctx, identity); err != nil {
		_ = rows.Close()
		c.removeActiveRead(identity)
		_ = tx.Rollback()
		return fail(fmt.Errorf("mysql read acknowledgement was not accepted: %w", err))
	}
	return &ReadStream{columns: query.ColumnsFromSQL(columnTypes), rows: rows, tx: tx, conn: conn, client: c, identity: identity}, identity, nil
}

func (c *Client) initializeReadDB(ctx context.Context, requireTLS bool) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.readConn != nil {
		if requireTLS {
			parsed, err := mysqldriver.ParseDSN(c.config.ToDBConnectionURI())
			if err != nil || parsed.TLSConfig == "" || parsed.TLSConfig == "false" {
				return errors.New("mysql governed read requires explicit TLS")
			}
		}
		return nil
	}
	dsn, err := governedReadDSN(c.config.ToDBConnectionURI(), requireTLS)
	if err != nil {
		return err
	}
	readConn, err := sqlx.ConnectContext(ctx, "mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect mysql read pool: %w", err)
	}
	control, err := sqlx.ConnectContext(ctx, "mysql", dsn)
	if err != nil {
		_ = readConn.Close()
		return fmt.Errorf("failed to connect mysql control pool: %w", err)
	}
	control.SetMaxOpenConns(2)
	control.SetMaxIdleConns(2)
	c.readConn, c.control = readConn, control
	return nil
}

func governedReadDSN(raw string, requireTLS bool) (string, error) {
	parsed, err := mysqldriver.ParseDSN(raw)
	if err != nil {
		return "", fmt.Errorf("failed to parse mysql native DSN: %w", err)
	}
	if requireTLS && (parsed.TLSConfig == "" || parsed.TLSConfig == "false") {
		return "", errors.New("mysql governed read requires explicit TLS")
	}
	parsed.MultiStatements = false
	parsed.InterpolateParams = false
	return parsed.FormatDSN(), nil
}

func (c *Client) removeActiveRead(identity ReadIdentity) {
	c.readMutex.Lock()
	if active, ok := c.activeReads[identity.AttemptTag]; ok && active.identity == identity {
		delete(c.activeReads, identity.AttemptTag)
	}
	c.readMutex.Unlock()
}

type ReadStream struct {
	columns  []query.Column
	rows     *sql.Rows
	tx       *sqlx.Tx
	conn     *sqlx.Conn
	client   *Client
	identity ReadIdentity
	close    sync.Once
	closeErr error
}

func (s *ReadStream) Columns() []query.Column { return append([]query.Column(nil), s.columns...) }
func (s *ReadStream) Next() bool              { return s.rows.Next() }
func (s *ReadStream) Values() ([]any, error) {
	values := make([]any, len(s.columns))
	destinations := make([]any, len(values))
	for i := range values {
		destinations[i] = &values[i]
	}
	if err := s.rows.Scan(destinations...); err != nil {
		return nil, fmt.Errorf("failed to scan mysql result row: %w", err)
	}
	for i, value := range values {
		if bytes, ok := value.([]byte); ok {
			values[i] = append([]byte(nil), bytes...)
		}
	}
	return values, nil
}

func (s *ReadStream) Err() error {
	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("mysql result iteration failed: %w", err)
	}
	return nil
}

func (s *ReadStream) Close() error {
	s.close.Do(func() {
		rowsErr := s.rows.Close()
		txErr := s.tx.Rollback()
		connErr := s.conn.Close()
		s.client.removeActiveRead(s.identity)
		if errors.Is(txErr, sql.ErrTxDone) {
			txErr = nil
		}
		s.closeErr = errors.Join(rowsErr, txErr, connErr)
	})
	return s.closeErr
}
