package snowflake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/snowflakedb/gosnowflake"
)

var (
	ErrReadIdentity  = errors.New("snowflake read identity is invalid")
	ErrReadNotActive = errors.New("snowflake read is not confirmed active")
	readTagPattern   = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,96}$`)
	readUUIDPattern  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

type ReadState string

const (
	ReadStateRunning       ReadState = "running"
	ReadStateStopped       ReadState = "stopped"
	ReadStateIndeterminate ReadState = "indeterminate"
)

// ReadIdentity binds a logical Chartworks attempt to Snowflake's request,
// session, query-tag and native query identifiers. QueryID is empty only in
// the dispatch journal entry written before user SQL reaches the driver.
type ReadIdentity struct {
	RequestID string
	QueryID   string
	QueryTag  string
	Account   string
	Database  string
	SessionID int64
}

func (i ReadIdentity) validDispatch() bool {
	return readUUIDPattern.MatchString(i.RequestID) && readTagPattern.MatchString(i.QueryTag) && i.Account != "" && i.Database != "" && i.SessionID > 0
}

func (i ReadIdentity) validAcknowledged() bool {
	return i.validDispatch() && i.QueryID != "" && len(i.QueryID) <= 128
}

type ReadObserver interface {
	OnDispatch(context.Context, ReadIdentity) error
	OnAcknowledged(context.Context, ReadIdentity) error
}

// OpenRead performs exactly one logical query submission. In particular it
// does not use DB.withIdempotentRetry: ambiguous transport failures retain the
// deterministic request identity for reconciliation by the caller.
func (db *DB) OpenRead(ctx context.Context, queryObj *query.Query, attemptTag string, observer ReadObserver) (*ReadStream, ReadIdentity, error) {
	if queryObj == nil || observer == nil || !readTagPattern.MatchString(attemptTag) {
		return nil, ReadIdentity{}, ErrReadIdentity
	}
	if err := validateReadArgs(queryObj.Args); err != nil {
		return nil, ReadIdentity{}, err
	}
	pool, err := db.initializeDB(ctx)
	if err != nil {
		return nil, ReadIdentity{}, err
	}
	conn, err := pool.Connx(ctx)
	if err != nil {
		return nil, ReadIdentity{}, fmt.Errorf("failed to reserve snowflake read session: %w", err)
	}
	fail := func(err error) (*ReadStream, ReadIdentity, error) {
		_ = conn.Close()
		return nil, ReadIdentity{}, err
	}
	identity := ReadIdentity{QueryTag: "cw:" + attemptTag}
	if err = conn.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SESSION()").Scan(&identity.Account, &identity.Database, &identity.SessionID); err != nil {
		return fail(fmt.Errorf("failed to identify snowflake read session: %w", err))
	}
	identity.RequestID = deterministicRequestID(attemptTag, identity.Account, identity.Database).String()
	if !identity.validDispatch() {
		return fail(ErrReadIdentity)
	}
	if err = observer.OnDispatch(ctx, identity); err != nil {
		return fail(fmt.Errorf("snowflake read dispatch was not accepted: %w", err))
	}
	requestID := gosnowflake.ParseUUID(identity.RequestID)
	qid := make(chan string, 1)
	if db.queryIDChannel != nil {
		qid = db.queryIDChannel()
	}
	queryCtx := gosnowflake.WithStreamDownloader(gosnowflake.WithHigherPrecision(ctx))
	queryCtx = gosnowflake.WithRequestID(queryCtx, requestID)
	queryCtx = gosnowflake.WithQueryTag(queryCtx, identity.QueryTag)
	queryCtx = gosnowflake.WithQueryIDChan(queryCtx, qid)
	rows, err := conn.QueryContext(queryCtx, queryObj.String(), queryObj.Args...)
	if err != nil {
		return fail(fmt.Errorf("failed to execute snowflake read: %w", err))
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		return fail(fmt.Errorf("failed to retrieve snowflake result schema: %w", err))
	}
	identity.QueryID = <-qid
	if !identity.validAcknowledged() {
		_ = rows.Close()
		return fail(ErrReadIdentity)
	}
	if err = observer.OnAcknowledged(ctx, identity); err != nil {
		_ = rows.Close()
		return fail(fmt.Errorf("snowflake read acknowledgement was not accepted: %w", err))
	}
	return &ReadStream{columns: query.ColumnsFromSQL(columnTypes), rows: rows, conn: conn.Conn}, identity, nil
}

func deterministicRequestID(attemptTag, account, database string) gosnowflake.UUID {
	sum := sha256.Sum256([]byte("chartworks:snowflake:" + account + ":" + database + ":" + attemptTag))
	var id gosnowflake.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

func validateReadArgs(args []any) error {
	for _, arg := range args {
		switch value := arg.(type) {
		case nil, bool, string, []byte, int64, time.Time, *big.Int:
		case sql.NamedArg:
			if value.Name != "" || validateReadArgs([]any{value.Value}) != nil {
				return errors.New("snowflake governed read argument type is unsupported")
			}
		default:
			return errors.New("snowflake governed read argument type is unsupported")
		}
	}
	return nil
}

// ReadStatus reconciles against Snowflake history for the exact originating
// session and tag. Missing history is indeterminate, never proof of completion.
func (db *DB) ReadStatus(ctx context.Context, identity ReadIdentity) (ReadState, error) {
	if !identity.validAcknowledged() {
		return "", ErrReadIdentity
	}
	pool, err := db.initializeDB(ctx)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	var account, database string
	if err := pool.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE()").Scan(&account, &database); err != nil || account != identity.Account || database != identity.Database {
		if err == nil {
			err = errors.New("snowflake account or database identity changed")
		}
		return ReadStateIndeterminate, err
	}
	var status, tag string
	err = pool.QueryRowContext(ctx, `SELECT EXECUTION_STATUS, COALESCE(QUERY_TAG, '') FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(SESSION_ID => ?, RESULT_LIMIT => 1000)) WHERE QUERY_ID = ?`, identity.SessionID, identity.QueryID).Scan(&status, &tag)
	if errors.Is(err, sql.ErrNoRows) {
		return ReadStateIndeterminate, nil
	}
	if err != nil || tag != identity.QueryTag {
		if err == nil {
			err = errors.New("snowflake query tag identity changed")
		}
		return ReadStateIndeterminate, err
	}
	switch strings.ToUpper(status) {
	case "RUNNING", "QUEUED", "RESUMING_WAREHOUSE":
		return ReadStateRunning, nil
	case "SUCCESS", "FAIL", "FAILED_WITH_ERROR", "CANCELED":
		return ReadStateStopped, nil
	default:
		return ReadStateIndeterminate, nil
	}
}

func (db *DB) CancelRead(ctx context.Context, identity ReadIdentity) error {
	state, err := db.ReadStatus(ctx, identity)
	if err != nil {
		return err
	}
	if state == ReadStateStopped {
		return nil
	}
	if state != ReadStateRunning {
		return ErrReadNotActive
	}
	pool, err := db.initializeDB(ctx)
	if err != nil {
		return err
	}
	var result string
	if err = pool.QueryRowContext(ctx, "SELECT SYSTEM$CANCEL_QUERY(?)", identity.QueryID).Scan(&result); err != nil {
		return fmt.Errorf("failed to cancel snowflake read: %w", err)
	}
	if result == "" {
		return errors.New("snowflake cancellation returned no confirmation")
	}
	return nil
}

type ReadStream struct {
	columns  []query.Column
	rows     *sql.Rows
	conn     *sql.Conn
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
		return nil, fmt.Errorf("failed to scan snowflake result row: %w", err)
	}
	for i, value := range values {
		values[i] = ownedReadValue(value)
	}
	return values, nil
}

func ownedReadValue(value any) any {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case *big.Int:
		return new(big.Int).Set(value)
	case big.Int:
		return *new(big.Int).Set(&value)
	case *big.Float:
		return new(big.Float).Copy(value)
	case big.Float:
		return *new(big.Float).Copy(&value)
	default:
		return value
	}
}

func (s *ReadStream) Err() error {
	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("snowflake result iteration failed: %w", err)
	}
	return nil
}

func (s *ReadStream) Close() error {
	s.close.Do(func() { s.closeErr = errors.Join(s.rows.Close(), s.conn.Close()) })
	return s.closeErr
}
