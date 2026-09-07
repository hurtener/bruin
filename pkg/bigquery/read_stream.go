package bigquery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/bruin-data/bruin/pkg/query"
	"google.golang.org/api/iterator"
)

var (
	ErrReadIdentity      = errors.New("bigquery read identity is invalid")
	ErrReadParameter     = errors.New("bigquery read parameter is unsupported or inexact")
	ErrReadSchema        = errors.New("bigquery read schema is unsupported")
	ErrReadClosed        = errors.New("bigquery read client is closed")
	ErrReadState         = errors.New("bigquery read has no current row")
	readAttemptPattern   = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	readJobPattern       = regexp.MustCompile(`^bruin_read_[a-f0-9]{64}$`)
	readProjectPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:.-]{0,254}$`)
	readLocationPattern  = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)
	readParameterPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
)

type (
	ReadIdentity struct{ ProjectID, Location, JobID string }
	ReadState    string
)

const (
	ReadStateRunning       ReadState = "running"
	ReadStateStopped       ReadState = "stopped"
	ReadStateIndeterminate ReadState = "indeterminate"
)

type ReadObserver interface {
	OnDispatch(context.Context, ReadIdentity) error
	OnAcknowledged(context.Context, ReadIdentity) error
}

// ReadOptions bounds native billing and job lifetime. Cancellation reconciliation
// is separately bounded; expiry does not imply that the server job stopped.
type ReadOptions struct {
	MaxBytesBilled            int64
	JobTimeout, CancelTimeout time.Duration
}

// ReadParameter provides an explicit scalar type, including typed NULLs and
// BIGNUMERIC. Arrays/structs are intentionally outside this first read contract.
type ReadParameter struct {
	Name, Type string
	Value      any
}

// ReadDryRun is bounded native planning evidence for one governed read. It is
// separate from OpenRead because a dry run has no executing job to journal or
// cancel. ReferencedTables and Columns come from BigQuery's native response.
type ReadDryRun struct {
	StatementType       string
	TotalBytesProcessed int64
	ReferencedTables    []string
	Columns             []query.Column
}

func (i ReadIdentity) valid() bool {
	return readProjectPattern.MatchString(i.ProjectID) && readLocationPattern.MatchString(i.Location) && readJobPattern.MatchString(i.JobID)
}
func readLabel(i ReadIdentity) string { return strings.TrimPrefix(i.JobID, "bruin_read_")[:60] }
func (c *Client) readClient(ctx context.Context) (*bq.Client, error) {
	if ctx == nil {
		return nil, ErrReadIdentity
	}
	c.clientMutex.Lock()
	closed := c.readClosed
	c.clientMutex.Unlock()
	if closed {
		return nil, ErrReadClosed
	}
	if err := c.createClient(ctx); err != nil {
		return nil, err
	}
	c.clientMutex.Lock()
	defer c.clientMutex.Unlock()
	if c.readClosed {
		return nil, ErrReadClosed
	}
	return c.client, nil
}

// Close prevents lazy recreation and releases the existing SDK client. Closing
// local streams/clients never claims remote cancellation; use CancelRead.
func (c *Client) Close() error {
	c.clientMutex.Lock()
	if c.readClosed {
		c.clientMutex.Unlock()
		return nil
	}
	c.readClosed = true
	client := c.client
	c.client = nil
	c.clientMutex.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}

func (c *Client) readIdentity(attemptTag string) (ReadIdentity, error) {
	if c.config == nil || !readAttemptPattern.MatchString(attemptTag) {
		return ReadIdentity{}, ErrReadIdentity
	}
	digest := sha256.Sum256([]byte(attemptTag))
	id := ReadIdentity{ProjectID: c.config.ProjectID, Location: c.config.Location, JobID: "bruin_read_" + hex.EncodeToString(digest[:])}
	if !id.valid() {
		return ReadIdentity{}, ErrReadIdentity
	}
	return id, nil
}

func (c *Client) ownsRead(i ReadIdentity) bool {
	return c.config != nil && i.valid() && i.ProjectID == c.config.ProjectID && i.Location == c.config.Location
}

func matchesReadJob(job *bq.Job, i ReadIdentity) bool {
	if job == nil || job.ProjectID() != i.ProjectID || job.Location() != i.Location || job.ID() != i.JobID {
		return false
	}
	config, err := job.Config()
	if err != nil {
		return false
	}
	q, ok := config.(*bq.QueryConfig)
	return ok && !q.UseLegacySQL && q.Labels["bruin_read_attempt"] == readLabel(i)
}

// OpenRead uses jobs.insert with one deterministic identity. SDK transport
// retries retain that same job ID; this method never logically redispatches under
// another ID. Callers still own SQL validation and read-only cloud permissions.
func (c *Client) OpenRead(ctx context.Context, q *query.Query, attemptTag string, observer ReadObserver, options ReadOptions) (*ReadStream, ReadIdentity, error) {
	if ctx == nil || q == nil || observer == nil || strings.TrimSpace(q.Query) == "" || len(q.VariableDefinitions) != 0 || options.MaxBytesBilled < 1 || options.JobTimeout < time.Millisecond || options.JobTimeout > time.Hour {
		return nil, ReadIdentity{}, ErrReadParameter
	}
	identity, err := c.readIdentity(attemptTag)
	if err != nil {
		return nil, ReadIdentity{}, err
	}
	parameters, err := readParameters(q.Args)
	if err != nil {
		return nil, ReadIdentity{}, err
	}
	client, err := c.readClient(ctx)
	if err != nil {
		return nil, ReadIdentity{}, err
	}
	native := client.Query(q.String())
	native.JobID = identity.JobID
	native.AddJobIDSuffix = false
	native.Location = identity.Location
	native.UseLegacySQL = false
	native.DisableQueryCache = true
	native.MaxBytesBilled = options.MaxBytesBilled
	if c.config.MaxBillableBytes != nil && *c.config.MaxBillableBytes > 0 && *c.config.MaxBillableBytes < native.MaxBytesBilled {
		native.MaxBytesBilled = *c.config.MaxBillableBytes
	}
	native.JobTimeout = options.JobTimeout
	native.Parameters = parameters
	native.Labels = map[string]string{"bruin_read_attempt": readLabel(identity)}
	if err = observer.OnDispatch(ctx, identity); err != nil {
		return nil, ReadIdentity{}, fmt.Errorf("bigquery read dispatch was not accepted: %w", err)
	}
	work, cancel := context.WithCancel(ctx)
	job, err := native.Run(work)
	if err != nil {
		cancel()
		return nil, identity, fmt.Errorf("bigquery read dispatch outcome requires reconciliation: %w", err)
	}
	if !matchesReadJob(job, identity) {
		cancel()
		return nil, identity, ErrReadIdentity
	}
	rows, err := job.Read(work)
	if err != nil {
		cancel()
		return nil, identity, fmt.Errorf("bigquery read schema was not available: %w", err)
	}
	columns, err := readColumns(rows.Schema)
	if err != nil {
		cancel()
		return nil, identity, err
	}
	// A page hint limits buffered row count, not allocation of an individual wire
	// value. The caller must enforce its serialized result/cell byte budget.
	rows.PageInfo().MaxSize = 1
	if err = observer.OnAcknowledged(work, identity); err != nil {
		cancel()
		return nil, identity, fmt.Errorf("bigquery read acknowledgement was not accepted: %w", err)
	}
	return &ReadStream{rows: rows, columns: columns, cancel: cancel}, identity, nil
}

// DryRunRead binds the same closed scalar parameters as OpenRead and asks
// BigQuery to plan without executing the query. The native processed-byte limit
// remains an admission ceiling; callers still verify dependencies and schema.
func (c *Client) DryRunRead(ctx context.Context, q *query.Query, maxBytes int64) (ReadDryRun, error) {
	if ctx == nil || q == nil || strings.TrimSpace(q.Query) == "" || len(q.VariableDefinitions) != 0 || maxBytes < 1 {
		return ReadDryRun{}, ErrReadParameter
	}
	parameters, err := readParameters(q.Args)
	if err != nil {
		return ReadDryRun{}, err
	}
	client, err := c.readClient(ctx)
	if err != nil {
		return ReadDryRun{}, err
	}
	native := client.Query(q.String())
	native.DryRun = true
	native.UseLegacySQL = false
	native.DisableQueryCache = true
	native.Location = c.config.Location
	native.MaxBytesBilled = maxBytes
	native.Parameters = parameters
	job, err := native.Run(ctx)
	if err != nil {
		return ReadDryRun{}, fmt.Errorf("bigquery governed dry run failed: %w", err)
	}
	status := job.LastStatus()
	if status == nil || status.Err() != nil || status.Statistics == nil {
		return ReadDryRun{}, ErrReadSchema
	}
	stats, ok := status.Statistics.Details.(*bq.QueryStatistics)
	if !ok || stats == nil || stats.TotalBytesProcessed < 0 || stats.TotalBytesProcessed > maxBytes {
		return ReadDryRun{}, ErrReadSchema
	}
	columns, err := readColumns(stats.Schema)
	if err != nil {
		return ReadDryRun{}, err
	}
	out := ReadDryRun{StatementType: stats.StatementType, TotalBytesProcessed: stats.TotalBytesProcessed, Columns: columns}
	for _, table := range stats.ReferencedTables {
		if table == nil || table.ProjectID == "" || table.DatasetID == "" || table.TableID == "" {
			return ReadDryRun{}, ErrReadSchema
		}
		out.ReferencedTables = append(out.ReferencedTables, table.ProjectID+"."+table.DatasetID+"."+table.TableID)
	}
	return out, nil
}

func (c *Client) ReadStatus(ctx context.Context, identity ReadIdentity) (ReadState, error) {
	if !c.ownsRead(identity) {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	client, err := c.readClient(ctx)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	job, err := client.JobFromIDLocation(ctx, identity.JobID, identity.Location)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	if !matchesReadJob(job, identity) {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	status, err := job.Status(ctx)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	if status == nil {
		return ReadStateIndeterminate, nil
	}
	switch status.State {
	case bq.Done:
		return ReadStateStopped, nil
	case bq.Pending, bq.Running:
		return ReadStateRunning, nil
	default:
		return ReadStateIndeterminate, nil
	}
}

// CancelRead records no optimistic stop: only a terminal native job status can
// return stopped. Lost replies/expired reconciliation stay indeterminate.
func (c *Client) CancelRead(ctx context.Context, identity ReadIdentity, options ReadOptions) (ReadState, error) {
	if ctx == nil || !c.ownsRead(identity) || options.CancelTimeout < time.Millisecond || options.CancelTimeout > 30*time.Second {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	bounded, stop := context.WithTimeout(ctx, options.CancelTimeout)
	defer stop()
	client, err := c.readClient(bounded)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	job, err := client.JobFromIDLocation(bounded, identity.JobID, identity.Location)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	if !matchesReadJob(job, identity) {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	status, err := job.Status(bounded)
	if err == nil && status != nil && status.State == bq.Done {
		return ReadStateStopped, nil
	}
	cancelErr := job.Cancel(bounded)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, statusErr := c.ReadStatus(bounded, identity)
		if state == ReadStateStopped {
			return state, nil
		}
		if statusErr != nil {
			return ReadStateIndeterminate, errors.Join(cancelErr, statusErr)
		}
		select {
		case <-bounded.Done():
			return ReadStateIndeterminate, errors.Join(cancelErr, bounded.Err())
		case <-ticker.C:
		}
	}
}

func readParameters(args []any) ([]bq.QueryParameter, error) {
	out := make([]bq.QueryParameter, len(args))
	names := map[string]bool{}
	named := false
	for index, arg := range args {
		name := ""
		var value any
		var err error
		if explicit, ok := arg.(ReadParameter); ok {
			name = explicit.Name
			value, err = explicitReadParameter(explicit)
		} else {
			value, err = scalarReadParameter(arg)
		}
		if err != nil {
			return nil, err
		}
		if name != "" {
			if !readParameterPattern.MatchString(name) || names[strings.ToLower(name)] {
				return nil, ErrReadParameter
			}
			names[strings.ToLower(name)] = true
			named = true
		}
		out[index] = bq.QueryParameter{Name: name, Value: value}
	}
	if named && len(names) != len(args) {
		return nil, ErrReadParameter
	}
	return out, nil
}

func scalarReadParameter(value any) (any, error) {
	switch v := value.(type) {
	case string:
		if utf8.ValidString(v) {
			return v, nil
		}
	case bool, int64:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case []byte:
		return append([]byte(nil), v...), nil
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			return v, nil
		}
	case time.Time:
		if v.UTC().Year() >= 1 && v.UTC().Year() <= 9999 && v.Nanosecond()%1000 == 0 {
			return v, nil
		}
	case civil.Date:
		if v.IsValid() && v.Year >= 1 && v.Year <= 9999 {
			return v, nil
		}
	case civil.Time:
		if v.IsValid() && v.Nanosecond%1000 == 0 {
			return v, nil
		}
	case civil.DateTime:
		if v.IsValid() && v.Date.Year >= 1 && v.Date.Year <= 9999 && v.Time.Nanosecond%1000 == 0 {
			return v, nil
		}
	case *big.Rat:
		text, err := readDecimal(v, "NUMERIC")
		if err == nil {
			return &bq.QueryParameterValue{Type: bq.StandardSQLDataType{TypeKind: "NUMERIC"}, Value: text}, nil
		}
	}
	return nil, ErrReadParameter
}

func explicitReadParameter(p ReadParameter) (any, error) {
	kind := p.Type
	switch kind {
	case "STRING", "BYTES", "INT64", "FLOAT64", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "NUMERIC", "BIGNUMERIC":
	default:
		return nil, ErrReadParameter
	}
	if p.Value == nil {
		return &bq.QueryParameterValue{Type: bq.StandardSQLDataType{TypeKind: kind}, Value: bq.NullString{}}, nil
	}
	if kind == "NUMERIC" || kind == "BIGNUMERIC" {
		var rat *big.Rat
		switch value := p.Value.(type) {
		case *big.Rat:
			rat = value
		case string:
			var ok bool
			rat, ok = new(big.Rat).SetString(value)
			if !ok {
				return nil, ErrReadParameter
			}
		default:
			return nil, ErrReadParameter
		}
		text, err := readDecimal(rat, kind)
		if err != nil {
			return nil, err
		}
		return &bq.QueryParameterValue{Type: bq.StandardSQLDataType{TypeKind: kind}, Value: text}, nil
	}
	valid := false
	switch kind {
	case "STRING":
		_, valid = p.Value.(string)
	case "BYTES":
		_, valid = p.Value.([]byte)
	case "INT64":
		_, valid = p.Value.(int64)
	case "FLOAT64":
		_, valid = p.Value.(float64)
	case "BOOL":
		_, valid = p.Value.(bool)
	case "TIMESTAMP":
		_, valid = p.Value.(time.Time)
	case "DATE":
		_, valid = p.Value.(civil.Date)
	case "TIME":
		_, valid = p.Value.(civil.Time)
	case "DATETIME":
		_, valid = p.Value.(civil.DateTime)
	}
	if !valid {
		return nil, ErrReadParameter
	}
	value, err := scalarReadParameter(p.Value)
	if err != nil {
		return nil, err
	}
	return &bq.QueryParameterValue{Type: bq.StandardSQLDataType{TypeKind: kind}, Value: value}, nil
}

func readDecimal(value *big.Rat, kind string) (string, error) {
	if value == nil {
		return "", ErrReadParameter
	}
	scale := int64(9)
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	if kind == "BIGNUMERIC" {
		scale = 38
		limit.Lsh(big.NewInt(1), 255)
	}
	scaled := new(big.Int).Mul(value.Num(), new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil))
	coefficient, remainder := new(big.Int), new(big.Int)
	coefficient.QuoRem(scaled, value.Denom(), remainder)
	if remainder.Sign() != 0 || coefficient.Cmp(limit) >= 0 || coefficient.Cmp(new(big.Int).Neg(limit)) < 0 || kind == "NUMERIC" && new(big.Int).Abs(coefficient).Cmp(limit) >= 0 {
		return "", ErrReadParameter
	}
	return value.FloatString(int(scale)), nil
}

func readColumns(schema bq.Schema) ([]query.Column, error) {
	if len(schema) == 0 {
		return nil, ErrReadSchema
	}
	columns := make([]query.Column, len(schema))
	names := map[string]bool{}
	for index, field := range schema {
		if field == nil || field.Name == "" || names[field.Name] || field.Repeated || len(field.Schema) > 0 {
			return nil, ErrReadSchema
		}
		names[field.Name] = true
		switch field.Type {
		case bq.StringFieldType, bq.BytesFieldType, bq.IntegerFieldType, bq.FloatFieldType, bq.BooleanFieldType, bq.TimestampFieldType, bq.DateFieldType, bq.TimeFieldType, bq.DateTimeFieldType, bq.NumericFieldType, bq.BigNumericFieldType, bq.GeographyFieldType, bq.JSONFieldType:
		default:
			return nil, ErrReadSchema
		}
		columns[index] = query.Column{Name: field.Name, DatabaseType: string(field.Type), Nullable: !field.Required, NullableKnown: true, Length: field.MaxLength, LengthKnown: field.MaxLength > 0, Precision: field.Precision, Scale: field.Scale, DecimalKnown: field.Precision > 0}
	}
	return columns, nil
}

type ReadStream struct {
	rows    *bq.RowIterator
	columns []query.Column
	cancel  context.CancelFunc
	nextMu  sync.Mutex
	mu      sync.Mutex
	current []any
	err     error
	closed  bool
}

var _ query.RowStream = (*ReadStream)(nil)

func (s *ReadStream) Columns() []query.Column { return append([]query.Column(nil), s.columns...) }
func (s *ReadStream) Next() bool {
	s.nextMu.Lock()
	defer s.nextMu.Unlock()
	s.mu.Lock()
	if s.closed || s.err != nil {
		s.mu.Unlock()
		return false
	}
	s.current = nil
	s.mu.Unlock()
	var row []bq.Value
	err := s.rows.Next(&row)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if err != nil {
		if !errors.Is(err, iterator.Done) {
			s.err = err
		}
		return false
	}
	s.current = make([]any, len(row))
	for index, value := range row {
		copy, err := copyReadValue(value)
		if err != nil {
			s.err = err
			s.current = nil
			return false
		}
		s.current[index] = copy
	}
	return true
}

func copyReadValue(value any) (any, error) {
	switch v := value.(type) {
	case nil, string, bool, int64, time.Time, civil.Date, civil.Time, civil.DateTime:
		return v, nil
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			return v, nil
		}
	case []byte:
		return append([]byte(nil), v...), nil
	case *big.Rat:
		if v != nil {
			return new(big.Rat).Set(v), nil
		}
	}
	return nil, ErrReadSchema
}

func (s *ReadStream) Values() ([]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.current == nil {
		return nil, ErrReadState
	}
	out := make([]any, len(s.current))
	for index, value := range s.current {
		copy, err := copyReadValue(value)
		if err != nil {
			return nil, err
		}
		out[index] = copy
	}
	return out, nil
}
func (s *ReadStream) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *ReadStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	s.current = nil
	s.mu.Unlock()
	s.nextMu.Lock()
	s.nextMu.Unlock()
	return nil
}
