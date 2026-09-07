package databricks

// This leaf uses the documented Statement Execution API, not the SQL driver's
// private Thrift operation ID. Protocol: https://docs.databricks.com/api/statement-execution/v1
// A lost submit response has no recoverable native ID in this API. The caller
// retains the predispatch attempt and MUST NOT automatically submit it again.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/civil"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/databricks/databricks-sql-go/auth/pat"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var (
	ErrReadIdentity    = errors.New("databricks read identity is invalid")
	ErrReadParameter   = errors.New("databricks read parameter is unsupported or inexact")
	ErrReadProtocol    = errors.New("databricks statement protocol is invalid")
	ErrReadLimit       = errors.New("databricks read limit exceeded")
	ErrReadUnavailable = errors.New("databricks statement service unavailable")
	ErrReadClosed      = errors.New("databricks read client is closed")
	ErrReadFailed      = errors.New("databricks statement execution failed")
	readName           = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	readAttempt        = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	readWarehouse      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	readStatement      = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
	readDecimalType    = regexp.MustCompile(`^DECIMAL\(([1-9][0-9]?),([0-9]+)\)$`)
)

type (
	ReadIdentity struct{ Workspace, WarehouseID, AttemptTag, StatementID string }
	ReadObserver interface {
		OnDispatch(ctx context.Context, identity ReadIdentity) error
		OnAcknowledged(ctx context.Context, identity ReadIdentity) error
	}
)
type ReadState string

const (
	ReadStateRunning       ReadState = "running"
	ReadStateStopped       ReadState = "stopped"
	ReadStateIndeterminate ReadState = "indeterminate"
)

// MaxBytes is the native internal-result budget. MaxResponseBytes bounds each
// decoded HTTP body independently; callers still enforce serialized output caps.
type ReadOptions struct {
	MaxRows, MaxBytes, MaxResponseBytes  int64
	PollInterval, CancelTimeout, Timeout time.Duration
}

// Only explicit named parameters are supported by Statement Execution. Values
// are encoded separately from SQL. BINARY, arrays and structs are not accepted.
type ReadParameter struct {
	Name, Type string
	Value      any
}

func (db *DB) readConfiguration() (*Config, string, string, *http.Client, context.Context, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.readClosed {
		return nil, "", "", nil, nil, ErrReadClosed
	}
	if db.readConfig == nil {
		if db.config == nil {
			return nil, "", "", nil, nil, ErrReadIdentity
		}
		copied := *db.config
		host := copied.Host
		if host == "" || strings.ContainsAny(host, "/@?#\\") || strings.Contains(host, ":") {
			return nil, "", "", nil, nil, ErrReadIdentity
		}
		if copied.Port == 0 {
			copied.Port = 443
		}
		if copied.Port < 1 || copied.Port > 65535 {
			return nil, "", "", nil, nil, ErrReadIdentity
		}
		parts := strings.Split(strings.TrimPrefix(copied.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "sql" || parts[1] != "1.0" || (parts[2] != "warehouses" && parts[2] != "endpoints") || !readWarehouse.MatchString(parts[3]) {
			return nil, "", "", nil, nil, ErrReadIdentity
		}
		if !copied.UseOAuthM2M() && (copied.Token == "" || copied.ClientID != "" || copied.ClientSecret != "") {
			return nil, "", "", nil, nil, ErrReadIdentity
		}
		db.readConfig = &copied
		db.readContext, db.readCancel = context.WithCancel(context.Background())
		if db.readHTTP == nil {
			db.readHTTP = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
		}
		// Redirects never carry credentials or turn a read into another endpoint.
		client := *db.readHTTP
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		db.readHTTP = &client
	}
	c := db.readConfig
	return c, "https://" + net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), strings.Split(strings.TrimPrefix(c.Path, "/"), "/")[3], db.readHTTP, db.readContext, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.readClosed {
		db.mu.Unlock()
		return nil
	}
	db.readClosed = true
	cancel, client, conn := db.readCancel, db.readHTTP, db.conn
	db.conn = nil
	db.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if client != nil {
		client.CloseIdleConnections()
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (db *DB) readIdentity(attempt string) (ReadIdentity, error) {
	_, workspace, warehouse, _, _, err := db.readConfiguration()
	if err != nil {
		return ReadIdentity{}, err
	}
	if !readAttempt.MatchString(attempt) {
		return ReadIdentity{}, ErrReadIdentity
	}
	return ReadIdentity{Workspace: workspace, WarehouseID: warehouse, AttemptTag: attempt}, nil
}

func (db *DB) ownsRead(id ReadIdentity) bool {
	expected, err := db.readIdentity(id.AttemptTag)
	return err == nil && id.Workspace == expected.Workspace && id.WarehouseID == expected.WarehouseID && readStatement.MatchString(id.StatementID)
}

func validReadOptions(o ReadOptions) bool {
	return o.MaxRows > 0 && o.MaxRows <= 1000000 && o.MaxBytes > 0 && o.MaxBytes <= 25<<20 && o.MaxResponseBytes >= 1024 && o.MaxResponseBytes <= 32<<20 && o.PollInterval >= time.Millisecond && o.PollInterval <= time.Second && o.Timeout >= time.Millisecond && o.Timeout <= time.Hour
}

type statementStatus struct {
	State string `json:"state"`
}
type statementColumn struct {
	Name      string `json:"name"`
	TypeName  string `json:"type_name"`
	TypeText  string `json:"type_text"`
	Position  int    `json:"position"`
	Precision int64  `json:"type_precision"`
	Scale     int64  `json:"type_scale"`
}
type statementChunk struct {
	Index    int64           `json:"chunk_index"`
	Offset   int64           `json:"row_offset"`
	Count    int64           `json:"row_count"`
	Next     *int64          `json:"next_chunk_index"`
	Data     [][]*string     `json:"data_array"`
	External json.RawMessage `json:"external_links"`
}
type statementManifest struct {
	Format      string `json:"format"`
	Truncated   bool   `json:"truncated"`
	TotalRows   int64  `json:"total_row_count"`
	TotalChunks int64  `json:"total_chunk_count"`
	Schema      struct {
		Count   int               `json:"column_count"`
		Columns []statementColumn `json:"columns"`
	} `json:"schema"`
}
type statementResponse struct {
	ID       string             `json:"statement_id"`
	Status   statementStatus    `json:"status"`
	Manifest *statementManifest `json:"manifest"`
	Result   *statementChunk    `json:"result"`
}

func (db *DB) readRequest(ctx context.Context, method, path string, payload any, maxBytes int64, out any) error {
	if ctx == nil {
		return ErrReadIdentity
	}
	c, workspace, _, client, lifetime, err := db.readConfiguration()
	if err != nil {
		return err
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	// Client lifetime cancellation is additional to, never a replacement for,
	// the supplied request deadline.
	stop := context.AfterFunc(lifetime, cancel)
	defer stop()
	var body io.Reader
	if payload != nil {
		encoded, e := json.Marshal(payload)
		if e != nil {
			return ErrReadParameter
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(work, method, workspace+path, body)
	if err != nil {
		return ErrReadIdentity
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.UseOAuthM2M() {
		// Workspace M2M endpoint and Bruin's existing explicit credentials. Unlike
		// the driver's authenticator this token exchange inherits the request context.
		authConfig := clientcredentials.Config{ClientID: c.ClientID, ClientSecret: c.ClientSecret, TokenURL: workspace + "/oidc/v1/token", Scopes: []string{"all-apis"}, AuthStyle: oauth2.AuthStyleInHeader}
		token, err := authConfig.Token(context.WithValue(work, oauth2.HTTPClient, client))
		if err != nil {
			return ErrReadUnavailable
		}
		token.SetAuthHeader(req)
	} else if err = (&pat.PATAuth{AccessToken: c.Token}).Authenticate(req); err != nil {
		return ErrReadUnavailable
	}
	// No application retry, redirect or new native identity on ambiguous POST.
	response, err := client.Do(req)
	if err != nil {
		if work.Err() != nil {
			return work.Err()
		}
		return ErrReadUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ErrReadUnavailable
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return ErrReadUnavailable
	}
	if int64(len(encoded)) > maxBytes {
		return ErrReadLimit
	}
	if !utf8.Valid(encoded) {
		return ErrReadProtocol
	}
	if len(bytes.TrimSpace(encoded)) == 0 && out == nil {
		return nil
	}
	if !json.Valid(encoded) {
		return ErrReadProtocol
	}
	if out != nil && json.Unmarshal(encoded, out) != nil {
		return ErrReadProtocol
	}
	return nil
}

func (db *DB) OpenRead(ctx context.Context, q *query.Query, attempt string, observer ReadObserver, o ReadOptions) (*ReadStream, ReadIdentity, error) {
	if ctx == nil || q == nil || observer == nil || strings.TrimSpace(q.Query) == "" || len(q.Query) > 16<<20 || !utf8.ValidString(q.Query) || len(q.VariableDefinitions) != 0 || !validReadOptions(o) {
		return nil, ReadIdentity{}, ErrReadParameter
	}
	ctx, executionCancel := context.WithTimeout(ctx, o.Timeout)
	keep := false
	defer func() {
		if !keep {
			executionCancel()
		}
	}()
	parameters, err := readParameters(q.Args)
	if err != nil {
		return nil, ReadIdentity{}, err
	}
	id, err := db.readIdentity(attempt)
	if err != nil {
		return nil, id, err
	}
	c, _, _, _, _, err := db.readConfiguration()
	if err != nil {
		return nil, id, err
	}
	payload := map[string]any{"statement": q.String(), "warehouse_id": id.WarehouseID, "catalog": c.Catalog, "schema": c.Schema, "parameters": parameters, "format": "JSON_ARRAY", "disposition": "INLINE", "wait_timeout": "0s", "row_limit": o.MaxRows, "byte_limit": o.MaxBytes, "query_tags": []map[string]string{{"key": "bruin_read_attempt", "value": attempt}}}
	if c.Catalog == "" {
		delete(payload, "catalog")
	}
	if c.Schema == "" {
		delete(payload, "schema")
	}
	encoded, encodeErr := json.Marshal(payload)
	if encodeErr != nil || len(encoded) > 17<<20 {
		return nil, id, ErrReadLimit
	}
	if err = observer.OnDispatch(ctx, id); err != nil {
		return nil, ReadIdentity{}, err
	}
	var response statementResponse
	if err = db.readRequest(ctx, http.MethodPost, "/api/2.0/sql/statements", payload, o.MaxResponseBytes, &response); err != nil {
		return nil, id, err
	}
	if !readStatement.MatchString(response.ID) {
		return nil, id, ErrReadProtocol
	}
	id.StatementID = response.ID
	// Persist the actual API ID immediately, even if execution later fails or the
	// result schema is unsupported. This is the only ID used for status/cancel.
	if err = observer.OnAcknowledged(ctx, id); err != nil {
		return nil, id, err
	}
	for response.Status.State == "PENDING" || response.Status.State == "RUNNING" {
		if err = waitRead(ctx, o.PollInterval); err != nil {
			return nil, id, err
		}
		response, err = db.readResponse(ctx, id, o.MaxResponseBytes)
		if err != nil {
			return nil, id, err
		}
	}
	if response.Status.State != "SUCCEEDED" {
		return nil, id, ErrReadFailed
	}
	columns, err := readColumns(response.Manifest, o)
	if err != nil {
		return nil, id, err
	}
	if response.Result == nil {
		if response.Manifest.TotalRows != 0 {
			return nil, id, ErrReadProtocol
		}
		response.Result = &statementChunk{}
	}
	stream := &ReadStream{db: db, ctx: ctx, cancel: executionCancel, id: id, options: o, columns: columns, manifest: response.Manifest, chunk: response.Result}
	if err = stream.validateChunk(0, 0); err != nil {
		return nil, id, err
	}
	keep = true
	return stream, id, nil
}

func waitRead(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (db *DB) readResponse(ctx context.Context, id ReadIdentity, maxBytes int64) (statementResponse, error) {
	var r statementResponse
	if !db.ownsRead(id) {
		return r, ErrReadIdentity
	}
	err := db.readRequest(ctx, http.MethodGet, "/api/2.0/sql/statements/"+id.StatementID, nil, maxBytes, &r)
	if err == nil && r.ID != id.StatementID {
		err = ErrReadIdentity
	}
	return r, err
}

func (db *DB) ReadStatus(ctx context.Context, id ReadIdentity) (ReadState, error) {
	r, err := db.readResponse(ctx, id, 1<<20)
	if err != nil {
		return ReadStateIndeterminate, err
	}
	switch r.Status.State {
	case "PENDING", "RUNNING":
		return ReadStateRunning, nil
	case "SUCCEEDED", "FAILED", "CANCELED", "CLOSED":
		return ReadStateStopped, nil
	default:
		return ReadStateIndeterminate, ErrReadProtocol
	}
}

func (db *DB) CancelRead(ctx context.Context, id ReadIdentity, o ReadOptions) (ReadState, error) {
	if ctx == nil || !db.ownsRead(id) || o.CancelTimeout < time.Millisecond || o.CancelTimeout > 30*time.Second {
		return ReadStateIndeterminate, ErrReadIdentity
	}
	bounded, cancel := context.WithTimeout(ctx, o.CancelTimeout)
	defer cancel()
	state, err := db.ReadStatus(bounded, id)
	if state == ReadStateStopped {
		return state, nil
	}
	if err != nil {
		return ReadStateIndeterminate, err
	}
	cancelErr := db.readRequest(bounded, http.MethodPost, "/api/2.0/sql/statements/"+id.StatementID+"/cancel", nil, 1<<20, nil)
	for {
		state, err = db.ReadStatus(bounded, id)
		if state == ReadStateStopped {
			return state, nil
		}
		if err != nil {
			return ReadStateIndeterminate, errors.Join(cancelErr, err)
		}
		if err = waitRead(bounded, 25*time.Millisecond); err != nil {
			return ReadStateIndeterminate, errors.Join(cancelErr, err)
		}
	}
}

type statementParameter struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Value *string `json:"value"`
}

func readParameters(args []any) ([]statementParameter, error) {
	out := make([]statementParameter, len(args))
	names := map[string]bool{}
	for i, arg := range args {
		p, ok := arg.(ReadParameter)
		if !ok || !readName.MatchString(p.Name) || names[strings.ToLower(p.Name)] {
			return nil, ErrReadParameter
		}
		names[strings.ToLower(p.Name)] = true
		kind := p.Type
		if !readScalarType(kind) || kind == "BINARY" || kind == "NULL" {
			return nil, ErrReadParameter
		}
		out[i] = statementParameter{Name: p.Name, Type: kind}
		if p.Value == nil {
			continue
		}
		value, err := readParameterText(kind, p.Value)
		if err != nil {
			return nil, err
		}
		out[i].Value = &value
	}
	return out, nil
}

func readScalarType(kind string) bool {
	switch kind {
	case "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "BINARY", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "NULL":
		return true
	}
	_, _, ok := readDecimalParts(kind)
	return ok
}

func readDecimalParts(kind string) (int, int, bool) {
	match := readDecimalType.FindStringSubmatch(kind)
	if match == nil {
		return 0, 0, false
	}
	p, _ := strconv.Atoi(match[1])
	s, _ := strconv.Atoi(match[2])
	return p, s, p <= 38 && s <= p
}

func readParameterText(kind string, value any) (string, error) {
	switch kind {
	case "STRING":
		if v, ok := value.(string); ok && utf8.ValidString(v) {
			return v, nil
		}
	case "BOOLEAN":
		if v, ok := value.(bool); ok {
			return strconv.FormatBool(v), nil
		}
	case "TINYINT", "SMALLINT", "INT", "BIGINT":
		if v, ok := value.(int64); ok {
			bits := 64
			switch kind {
			case "TINYINT":
				bits = 8
			case "SMALLINT":
				bits = 16
			case "INT":
				bits = 32
			}
			s := strconv.FormatInt(v, 10)
			if _, err := strconv.ParseInt(s, 10, bits); err == nil {
				return s, nil
			}
		}
	case "FLOAT", "DOUBLE":
		if v, ok := value.(float64); ok && !math.IsNaN(v) && !math.IsInf(v, 0) {
			if kind == "FLOAT" && float64(float32(v)) != v {
				return "", ErrReadParameter
			}
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		}
	case "DATE":
		if v, ok := value.(civil.Date); ok && v.IsValid() && v.Year >= 1 && v.Year <= 9999 {
			return v.String(), nil
		}
	case "TIMESTAMP":
		if v, ok := value.(time.Time); ok && v.UTC().Year() >= 1 && v.UTC().Year() <= 9999 && v.Nanosecond()%1000 == 0 {
			return v.UTC().Format(time.RFC3339Nano), nil
		}
	case "TIMESTAMP_NTZ":
		if v, ok := value.(civil.DateTime); ok && v.IsValid() && v.Date.Year >= 1 && v.Date.Year <= 9999 && v.Time.Nanosecond%1000 == 0 {
			return v.String(), nil
		}
	default:
		if p, s, ok := readDecimalParts(kind); ok {
			if v, ok := value.(*big.Rat); ok && v != nil {
				factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(s)), nil)
				scaled := new(big.Rat).Mul(v, new(big.Rat).SetInt(factor))
				limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil)
				if scaled.IsInt() && new(big.Int).Abs(scaled.Num()).Cmp(limit) < 0 {
					return v.FloatString(s), nil
				}
			}
		}
	}
	return "", ErrReadParameter
}

func readColumns(m *statementManifest, o ReadOptions) ([]query.Column, error) {
	if m == nil || m.Format != "JSON_ARRAY" || m.Truncated || m.TotalRows < 0 || m.TotalRows > o.MaxRows {
		return nil, ErrReadLimit
	}
	if len(m.Schema.Columns) == 0 || m.Schema.Count != len(m.Schema.Columns) || m.TotalChunks < 0 || m.TotalChunks > o.MaxRows+1 {
		return nil, ErrReadProtocol
	}
	out := make([]query.Column, len(m.Schema.Columns))
	names := map[string]bool{}
	for i, c := range m.Schema.Columns {
		if c.Position != i || c.Name == "" || names[c.Name] || !readScalarType(c.TypeText) || c.TypeText == "BINARY" {
			return nil, ErrReadProtocol
		}
		names[c.Name] = true
		out[i] = query.Column{Name: c.Name, DatabaseType: c.TypeText}
		if p, s, ok := readDecimalParts(c.TypeText); ok {
			if c.TypeName != "DECIMAL" || c.Precision != int64(p) || c.Scale != int64(s) {
				return nil, ErrReadProtocol
			}
			out[i].Precision = int64(p)
			out[i].Scale = int64(s)
			out[i].DecimalKnown = true
		}
	}
	return out, nil
}

// ReadStream retains its opening context because the shared row iterator
// interface has no context argument on Next. Close cancels that context first.
type ReadStream struct {
	db       *DB
	ctx      context.Context
	cancel   context.CancelFunc
	id       ReadIdentity
	options  ReadOptions
	columns  []query.Column
	manifest *statementManifest
	chunk    *statementChunk
	mu       sync.Mutex
	current  []any
	offset   int
	read     int64
	err      error
	closed   bool
}

var _ query.RowStream = (*ReadStream)(nil)

func (s *ReadStream) Columns() []query.Column { return append([]query.Column(nil), s.columns...) }
func (s *ReadStream) validateChunk(index, offset int64) error {
	c := s.chunk
	if c == nil || c.Index != index || c.Offset != offset || c.Count != int64(len(c.Data)) || c.Count < 0 || offset+c.Count > s.manifest.TotalRows || len(c.External) > 0 {
		return ErrReadProtocol
	}
	if c.Next != nil && (*c.Next != index+1 || *c.Next >= s.manifest.TotalChunks || c.Count == 0) {
		return ErrReadProtocol
	}
	if c.Next == nil && (offset+c.Count != s.manifest.TotalRows || (s.manifest.TotalRows > 0 && index+1 != s.manifest.TotalChunks)) {
		return ErrReadProtocol
	}
	return nil
}

func (s *ReadStream) Next() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.err != nil {
		return false
	}
	s.current = nil
	if s.offset == len(s.chunk.Data) {
		if s.chunk.Next == nil {
			return false
		}
		index := *s.chunk.Next
		var next statementChunk
		err := s.db.readRequest(s.ctx, http.MethodGet, fmt.Sprintf("/api/2.0/sql/statements/%s/result/chunks/%d", s.id.StatementID, index), nil, s.options.MaxResponseBytes, &next)
		if err != nil {
			s.err = err
			return false
		}
		s.chunk = &next
		s.offset = 0
		if err = s.validateChunk(index, s.read); err != nil {
			s.err = err
			return false
		}
	}
	row := s.chunk.Data[s.offset]
	s.offset++
	s.read++
	if len(row) != len(s.columns) {
		s.err = ErrReadProtocol
		return false
	}
	s.current = make([]any, len(row))
	for i, cell := range row {
		if cell == nil {
			continue
		}
		v, err := readValue(s.columns[i].DatabaseType, *cell)
		if err != nil {
			s.err = err
			s.current = nil
			return false
		}
		s.current[i] = v
	}
	return true
}

func readValue(kind, text string) (any, error) {
	switch kind {
	case "STRING":
		return text, nil
	case "BOOLEAN":
		if text == "true" || text == "false" {
			return text == "true", nil
		}
	case "TINYINT", "SMALLINT", "INT", "BIGINT":
		bits := 64
		switch kind {
		case "TINYINT":
			bits = 8
		case "SMALLINT":
			bits = 16
		case "INT":
			bits = 32
		}
		v, err := strconv.ParseInt(text, 10, bits)
		if err == nil {
			return v, nil
		}
	case "FLOAT", "DOUBLE":
		bits := 64
		if kind == "FLOAT" {
			bits = 32
		}
		v, err := strconv.ParseFloat(text, bits)
		if err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) {
			return v, nil
		}
	case "DATE":
		v, err := civil.ParseDate(text)
		if err == nil && v.Year >= 1 && v.Year <= 9999 {
			return v, nil
		}
	case "TIMESTAMP":
		v, err := time.Parse(time.RFC3339Nano, strings.Replace(text, " ", "T", 1))
		if err == nil && v.Nanosecond()%1000 == 0 && v.UTC().Year() >= 1 && v.UTC().Year() <= 9999 {
			return v, nil
		}
	case "TIMESTAMP_NTZ":
		v, err := civil.ParseDateTime(strings.Replace(text, " ", "T", 1))
		if err == nil && v.Time.Nanosecond%1000 == 0 && v.Date.Year >= 1 && v.Date.Year <= 9999 {
			return v, nil
		}
	default:
		if _, _, ok := readDecimalParts(kind); ok {
			v, ok := new(big.Rat).SetString(text)
			if ok {
				if _, err := readParameterText(kind, v); err == nil {
					return v, nil
				}
			}
		}
	}
	return nil, ErrReadProtocol
}

func (s *ReadStream) Values() ([]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.current == nil {
		return nil, ErrReadProtocol
	}
	out := append([]any(nil), s.current...)
	for i, v := range out {
		if rat, ok := v.(*big.Rat); ok {
			out[i] = new(big.Rat).Set(rat)
		}
	}
	return out, nil
}
func (s *ReadStream) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *ReadStream) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.current = nil
	s.chunk = nil
	return nil
}
