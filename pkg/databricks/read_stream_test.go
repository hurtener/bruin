package databricks

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/bruin-data/bruin/pkg/query"
)

const fixtureStatementID = "01234567-1234-1234-1234-0123456789ab"

type statementObserverFixture struct {
	denied        bool
	dispatch, ack int
	id            ReadIdentity
}

func (o *statementObserverFixture) OnDispatch(_ context.Context, id ReadIdentity) error {
	o.dispatch++
	if id.StatementID != "" {
		return errors.New("premature native ID")
	}
	if o.denied {
		return errors.New("denied")
	}
	return nil
}

func (o *statementObserverFixture) OnAcknowledged(_ context.Context, id ReadIdentity) error {
	o.ack++
	o.id = id
	return nil
}

type statementProtocolFixture struct {
	mu                                                                                      sync.Mutex
	submits, cancels, tokens                                                                int
	payload                                                                                 map[string]any
	state                                                                                   string
	lostSubmit, lostCancel, unknownCancel, empty, truncated, overflow, mismatch, blockChunk bool
	entered                                                                                 chan struct{}
}

func (f *statementProtocolFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	var response any
	switch {
	case r.URL.Path == "/oidc/v1/token":
		f.tokens++
		user, password, ok := r.BasicAuth()
		if !ok || user != "synthetic-client" || password != "synthetic-secret" {
			http.Error(w, "bad auth", http.StatusUnauthorized)
			return
		}
		response = map[string]any{"access_token": "synthetic-token", "token_type": "Bearer", "expires_in": 3600}
	case r.Header.Get("Authorization") != "Bearer synthetic-token":
		http.Error(w, "bad auth", http.StatusUnauthorized)
		return
	case r.URL.Path == "/api/2.0/sql/statements" && r.Method == http.MethodPost:
		f.submits++
		if err := json.NewDecoder(r.Body).Decode(&f.payload); err != nil {
			panic(err)
		}
		if f.lostSubmit {
			http.Error(w, "uncertain", http.StatusServiceUnavailable)
			return
		}
		response = map[string]any{"statement_id": fixtureStatementID, "status": map[string]string{"state": "PENDING"}}
	case strings.HasSuffix(r.URL.Path, "/cancel"):
		f.cancels++
		if !f.unknownCancel {
			f.state = "CANCELED"
		}
		if f.lostCancel {
			http.Error(w, "lost reply", http.StatusServiceUnavailable)
			return
		}
		response = map[string]any{}
	case strings.Contains(r.URL.Path, "/result/chunks/"):
		if !strings.HasSuffix(r.URL.Path, "/1") {
			http.Error(w, "wrong chunk", http.StatusBadRequest)
			return
		}
		if f.blockChunk {
			close(f.entered)
			f.mu.Unlock()
			<-r.Context().Done()
			f.mu.Lock()
			return
		}
		response = map[string]any{"chunk_index": 1, "row_offset": 1, "row_count": 1, "data_array": [][]any{{"9007199254740994", "3.140000000", "2026-09-07T12:13:14.123456Z", "2026-09-07T12:13:14.123456", "2026-09-07", nil}}}
	case r.URL.Path == "/api/2.0/sql/statements/"+fixtureStatementID:
		if f.overflow {
			_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
			return
		}
		id := fixtureStatementID
		if f.mismatch {
			id = "ffffffff-1234-1234-1234-0123456789ab"
		}
		state := f.state
		if state == "" {
			state = "SUCCEEDED"
		}
		response = map[string]any{"statement_id": id, "status": map[string]string{"state": state}}
		if state == "SUCCEEDED" {
			total, chunks := 2, 2
			if f.empty {
				total, chunks = 0, 0
			}
			columns := []any{map[string]any{"name": "n", "type_name": "LONG", "type_text": "BIGINT", "position": 0}, map[string]any{"name": "d", "type_name": "DECIMAL", "type_text": "DECIMAL(29,9)", "type_precision": 29, "type_scale": 9, "position": 1}, map[string]any{"name": "ts", "type_name": "TIMESTAMP", "type_text": "TIMESTAMP", "position": 2}, map[string]any{"name": "ntz", "type_name": "TIMESTAMP", "type_text": "TIMESTAMP_NTZ", "position": 3}, map[string]any{"name": "date", "type_name": "DATE", "type_text": "DATE", "position": 4}, map[string]any{"name": "nil", "type_name": "STRING", "type_text": "STRING", "position": 5}}
			object := response.(map[string]any)
			object["manifest"] = map[string]any{"format": "JSON_ARRAY", "truncated": f.truncated, "total_row_count": total, "total_chunk_count": chunks, "schema": map[string]any{"column_count": len(columns), "columns": columns}}
			if !f.empty {
				object["result"] = map[string]any{"chunk_index": 0, "row_offset": 0, "row_count": 1, "next_chunk_index": 1, "next_chunk_internal_link": "https://untrusted.invalid/do-not-follow", "data_array": [][]any{{"9007199254740993", "12345678901234567890.123456789", "2026-09-07T12:13:14.123456Z", "2026-09-07T12:13:14.123456", "2026-09-07", nil}}}
			}
		}
	default:
		http.Error(w, "unexpected endpoint", http.StatusBadRequest)
		return
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		panic(err)
	}
}

func newStatementFixture(t *testing.T, f *statementProtocolFixture, oauth bool) *DB {
	t.Helper()
	server := httptest.NewTLSServer(f)
	t.Cleanup(server.Close)
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(endpoint.Port())
	if err != nil {
		t.Fatal(err)
	}
	config := &Config{Host: endpoint.Hostname(), Port: port, Path: "/sql/1.0/warehouses/synthetic-warehouse", Catalog: "synthetic", Schema: "analytics", Token: "synthetic-token"}
	if oauth {
		config.Token = ""
		config.ClientID = "synthetic-client"
		config.ClientSecret = "synthetic-secret"
	}
	db, err := NewDB(config)
	if err != nil {
		t.Fatal(err)
	}
	db.readHTTP = server.Client()
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func statementOptions() ReadOptions {
	return ReadOptions{MaxRows: 10, MaxBytes: 10000, MaxResponseBytes: 2048, PollInterval: time.Millisecond, CancelTimeout: 100 * time.Millisecond, Timeout: time.Second}
}

func TestStatementReadProtocol(t *testing.T) {
	t.Run("pre-dispatch denial prevents submission", func(t *testing.T) {
		f := &statementProtocolFixture{}
		db := newStatementFixture(t, f, false)
		o := &statementObserverFixture{denied: true}
		_, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "attempt", o, statementOptions())
		if err == nil || f.submits != 0 || o.ack != 0 {
			t.Fatalf("denial: %v %+v", err, f)
		}
	})
	t.Run("exact named parameters and native chunk values", func(t *testing.T) {
		f := &statementProtocolFixture{}
		db := newStatementFixture(t, f, false)
		o := &statementObserverFixture{}
		decimal, _ := new(big.Rat).SetString("12345678901234567890.123456789")
		sql := "SELECT :n,:d,:missing"
		stream, id, err := db.OpenRead(context.Background(), &query.Query{Query: sql, Args: []any{ReadParameter{Name: "n", Type: "BIGINT", Value: int64(9007199254740993)}, ReadParameter{Name: "d", Type: "DECIMAL(29,9)", Value: decimal}, ReadParameter{Name: "missing", Type: "STRING"}}}, "attempt", o, statementOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		if o.dispatch != 1 || o.ack != 1 || id.StatementID != fixtureStatementID || o.id != id || len(stream.Columns()) != 6 {
			t.Fatalf("identity/schema: %+v %+v", id, o)
		}
		if f.payload["statement"] != sql || f.payload["wait_timeout"] != "0s" || f.payload["disposition"] != "INLINE" {
			t.Fatal("submission contract changed")
		}
		parameters := f.payload["parameters"].([]any)
		if parameters[0].(map[string]any)["value"] != "9007199254740993" || parameters[1].(map[string]any)["value"] != "12345678901234567890.123456789" || parameters[2].(map[string]any)["value"] != nil {
			t.Fatalf("parameter precision: %#v", parameters)
		}
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		values, err := stream.Values()
		if err != nil {
			t.Fatal(err)
		}
		timestamp, _ := time.Parse(time.RFC3339Nano, "2026-09-07T12:13:14.123456Z")
		local, _ := civil.ParseDateTime("2026-09-07T12:13:14.123456")
		if values[0] != int64(9007199254740993) || values[1].(*big.Rat).Cmp(decimal) != 0 || values[2] != timestamp || values[3] != local || values[4] != (civil.Date{Year: 2026, Month: 9, Day: 7}) || values[5] != nil {
			t.Fatalf("values: %#v", values)
		}
		values[1].(*big.Rat).SetInt64(0)
		again, _ := stream.Values()
		if again[1].(*big.Rat).Cmp(decimal) != 0 {
			t.Fatal("value ownership leaked")
		}
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		values, _ = stream.Values()
		if values[0] != int64(9007199254740994) || stream.Next() || stream.Err() != nil {
			t.Fatal("chunk sequence failed")
		}
	})
	t.Run("empty schema and OAuth M2M", func(t *testing.T) {
		f := &statementProtocolFixture{empty: true}
		db := newStatementFixture(t, f, true)
		stream, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT 1 WHERE FALSE"}, "empty", &statementObserverFixture{}, statementOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		if len(stream.Columns()) != 6 || stream.Next() || stream.Err() != nil || f.tokens != 2 {
			t.Fatal("empty schema/auth failed")
		}
	})
	for _, mode := range []string{"lost-submit", "mismatched-id", "truncated", "wire-limit"} {
		t.Run(mode, func(t *testing.T) {
			f := &statementProtocolFixture{lostSubmit: mode == "lost-submit", mismatch: mode == "mismatched-id", truncated: mode == "truncated", overflow: mode == "wire-limit"}
			db := newStatementFixture(t, f, false)
			o := &statementObserverFixture{}
			stream, id, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "attempt", o, statementOptions())
			if err == nil || stream != nil || f.submits != 1 {
				t.Fatalf("failure not retained: %v", err)
			}
			if mode == "lost-submit" {
				if id.StatementID != "" || o.ack != 0 {
					t.Fatal("fabricated native ID")
				}
			} else if id.StatementID != fixtureStatementID || o.ack != 1 {
				t.Fatal("lost native ID")
			}
		})
	}
	for _, mode := range []string{"confirmed", "lost-cancel-reply", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := &statementProtocolFixture{empty: true, lostCancel: mode == "lost-cancel-reply", unknownCancel: mode == "unknown"}
			db := newStatementFixture(t, f, false)
			stream, id, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "cancel", &statementObserverFixture{}, statementOptions())
			if err != nil {
				t.Fatal(err)
			}
			_ = stream.Close()
			f.mu.Lock()
			f.state = "RUNNING"
			f.mu.Unlock()
			state, err := db.CancelRead(context.Background(), id, statementOptions())
			if mode == "unknown" {
				if state != ReadStateIndeterminate || err == nil {
					t.Fatalf("uncertain: %s %v", state, err)
				}
			} else if state != ReadStateStopped || err != nil {
				t.Fatalf("terminal: %s %v", state, err)
			}
			if f.submits != 1 || f.cancels != 1 {
				t.Fatal("logical retry or missing cancel")
			}
			id.WarehouseID = "foreign"
			if _, err = db.ReadStatus(context.Background(), id); !errors.Is(err, ErrReadIdentity) {
				t.Fatal("foreign identity accepted")
			}
		})
	}
	t.Run("stream close cancels local chunk fetch", func(t *testing.T) {
		f := &statementProtocolFixture{blockChunk: true, entered: make(chan struct{})}
		db := newStatementFixture(t, f, false)
		stream, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "close", &statementObserverFixture{}, statementOptions())
		if err != nil {
			t.Fatal(err)
		}
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		done := make(chan struct{})
		go func() { defer close(done); stream.Next() }()
		<-f.entered
		if err = stream.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("close did not join local fetch")
		}
		if f.cancels != 0 {
			t.Fatal("local close claimed remote cancel")
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = db.readIdentity("new"); !errors.Is(err, ErrReadClosed) {
			t.Fatal("closed client recreated")
		}
	})
}

func TestStatementParameterExactness(t *testing.T) {
	badDecimal := new(big.Rat).SetFrac64(1, 1000)
	for _, parameter := range []ReadParameter{{Name: "x", Type: "BINARY", Value: []byte{0, 255}}, {Name: "x", Type: "ARRAY", Value: []int{1}}, {Name: "x", Type: "TIMESTAMP", Value: time.Unix(1, 1)}, {Name: "x", Type: "DECIMAL(3,2)", Value: badDecimal}, {Name: "x", Type: "INT", Value: int64(1 << 40)}, {Name: "x", Type: "STRING", Value: "\xff"}} {
		if _, err := readParameters([]any{parameter}); !errors.Is(err, ErrReadParameter) {
			t.Fatalf("inexact parameter accepted: %#v", parameter)
		}
	}
	if _, err := readParameters([]any{int64(1)}); err == nil {
		t.Fatal("positional parameter accepted")
	}
	if _, err := readValue("TIMESTAMP", "2026-09-07 12:13:14"); err == nil {
		t.Fatal("invented timestamp timezone")
	}
	if _, err := readValue("BINARY", "AP8="); err == nil {
		t.Fatal("guessed binary codec")
	}
}
