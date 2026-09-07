package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/bruin-data/bruin/pkg/query"
	"google.golang.org/api/option"
)

type readObserverFixture struct {
	denied        bool
	dispatch, ack int
}

func (o *readObserverFixture) OnDispatch(context.Context, ReadIdentity) error {
	o.dispatch++
	if o.denied {
		return errors.New("denied")
	}
	return nil
}

func (o *readObserverFixture) OnAcknowledged(context.Context, ReadIdentity) error {
	o.ack++
	return nil
}

type readProtocolFixture struct {
	mu            sync.Mutex
	insert        int
	job           map[string]any
	state         string
	mismatch      bool
	cancelUnknown bool
	cancelCount   int
	empty         bool
	temporal      bool
}

func (f *readProtocolFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	var result any
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/jobs"):
		f.insert++
		if err := json.NewDecoder(r.Body).Decode(&f.job); err != nil {
			panic(err)
		}
		f.job["status"] = map[string]any{"state": f.state}
		if f.mismatch {
			f.job["jobReference"].(map[string]any)["location"] = "EU"
		}
		result = f.job
	case strings.HasSuffix(r.URL.Path, "/cancel"):
		f.cancelCount++
		if !f.cancelUnknown {
			f.state = "DONE"
		}
		result = map[string]any{"job": f.job}
	case strings.Contains(r.URL.Path, "/queries/"):
		fields := []any{map[string]any{"name": "n", "type": "INTEGER"}, map[string]any{"name": "d", "type": "NUMERIC"}, map[string]any{"name": "b", "type": "BYTES"}, map[string]any{"name": "z", "type": "STRING"}}
		response := map[string]any{"jobComplete": true, "schema": map[string]any{"fields": fields}, "totalRows": "0"}
		if !f.empty {
			response["totalRows"] = "1"
			if r.URL.Query().Get("maxResults") != "0" {
				response["rows"] = []any{map[string]any{"f": []any{map[string]any{"v": "9007199254740993"}, map[string]any{"v": "12345678901234567890.123456789"}, map[string]any{"v": "AP8="}, map[string]any{"v": nil}}}}
			}
		}

		if f.temporal {
			response["schema"] = map[string]any{"fields": []any{map[string]any{"name": "ts", "type": "TIMESTAMP"}, map[string]any{"name": "date", "type": "DATE"}, map[string]any{"name": "clock", "type": "TIME"}, map[string]any{"name": "local", "type": "DATETIME"}}}
			if r.URL.Query().Get("maxResults") != "0" {
				response["rows"] = []any{map[string]any{"f": []any{map[string]any{"v": "1788783194123456"}, map[string]any{"v": "2026-09-07"}, map[string]any{"v": "12:13:14.123456"}, map[string]any{"v": "2026-09-07T12:13:14.123456"}}}}
			}
		}
		result = response
	case strings.Contains(r.URL.Path, "/jobs/"):
		if f.job == nil {
			http.Error(w, "missing", 404)
			return
		}
		f.job["status"] = map[string]any{"state": f.state}
		result = f.job
	default:
		http.Error(w, "unexpected", 400)
		return
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		panic(err)
	}
}

func readFixtureClient(t *testing.T, f *readProtocolFixture) *Client {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	native, err := bq.NewClient(context.Background(), "synthetic-project", option.WithEndpoint(server.URL), option.WithoutAuthentication(), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{client: native, config: &Config{ProjectID: "synthetic-project", Location: "US"}}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func readFixtureOptions() ReadOptions {
	return ReadOptions{MaxBytesBilled: 1000, JobTimeout: time.Second, CancelTimeout: 100 * time.Millisecond}
}

func TestReadProtocol(t *testing.T) {
	t.Run("dispatch denied before SDK submission", func(t *testing.T) {
		f := &readProtocolFixture{state: "DONE"}
		c := readFixtureClient(t, f)
		o := &readObserverFixture{denied: true}
		_, _, err := c.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "attempt", o, readFixtureOptions())
		if err == nil || f.insert != 0 || o.ack != 0 {
			t.Fatalf("denial: %v %+v", err, f)
		}
	})
	t.Run("typed binding and exact streamed values", func(t *testing.T) {
		f := &readProtocolFixture{state: "DONE"}
		c := readFixtureClient(t, f)
		o := &readObserverFixture{}
		sql := "SELECT @n, @d, @z"
		stream, id, err := c.OpenRead(context.Background(), &query.Query{Query: sql, Args: []any{ReadParameter{Name: "n", Type: "INT64", Value: int64(9007199254740993)}, ReadParameter{Name: "d", Type: "NUMERIC", Value: "12345678901234567890.123456789"}, ReadParameter{Name: "z", Type: "STRING"}}}, "attempt", o, readFixtureOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		if !id.valid() || o.dispatch != 1 || o.ack != 1 || len(stream.Columns()) != 4 {
			t.Fatalf("admission: %+v %+v", id, o)
		}
		native := f.job["configuration"].(map[string]any)["query"].(map[string]any)
		if native["query"] != sql || native["useLegacySql"] != false {
			t.Fatalf("binding: %#v", native)
		}
		params := native["queryParameters"].([]any)
		first := params[0].(map[string]any)["parameterValue"].(map[string]any)

		second := params[1].(map[string]any)["parameterValue"].(map[string]any)
		third := params[2].(map[string]any)["parameterValue"].(map[string]any)
		if second["value"] != "12345678901234567890.123456789" || third["value"] != nil {
			t.Fatalf("decimal/null binding changed: %#v %#v", second, third)
		}
		if first["value"] != "9007199254740993" {
			t.Fatalf("integer altered: %#v", first)
		}
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		values, err := stream.Values()
		if err != nil {
			t.Fatal(err)
		}
		expected, _ := new(big.Rat).SetString("12345678901234567890.123456789")
		if values[0] != int64(9007199254740993) || values[1].(*big.Rat).Cmp(expected) != 0 || string(values[2].([]byte)) != "\x00\xff" || values[3] != nil {
			t.Fatalf("values: %#v", values)
		}
		values[2].([]byte)[0] = 12
		values[1].(*big.Rat).SetInt64(0)
		again, _ := stream.Values()
		if again[2].([]byte)[0] != 0 || again[1].(*big.Rat).Cmp(expected) != 0 {
			t.Fatal("mutable value ownership leaked")
		}
		if stream.Next() || stream.Err() != nil {
			t.Fatalf("EOF: %v", stream.Err())
		}
	})
	t.Run("empty results retain schema", func(t *testing.T) {
		f := &readProtocolFixture{state: "DONE", empty: true}
		c := readFixtureClient(t, f)
		stream, _, err := c.OpenRead(context.Background(), &query.Query{Query: "SELECT 1 WHERE FALSE"}, "empty", &readObserverFixture{}, readFixtureOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		if len(stream.Columns()) != 4 || stream.Next() || stream.Err() != nil {
			t.Fatal("empty schema lost")
		}
	})
	t.Run("mismatched server identity rejected", func(t *testing.T) {
		f := &readProtocolFixture{state: "DONE", mismatch: true}
		c := readFixtureClient(t, f)
		o := &readObserverFixture{}
		_, id, err := c.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "mismatch", o, readFixtureOptions())
		if !errors.Is(err, ErrReadIdentity) || !id.valid() || o.ack != 0 {
			t.Fatalf("identity: %+v %v", id, err)
		}
	})
	for _, unknown := range []bool{false, true} {
		name := "confirmed native cancellation"
		if unknown {
			name = "unconfirmed cancellation remains unknown"
		}
		t.Run(name, func(t *testing.T) {
			f := &readProtocolFixture{state: "DONE", empty: true, cancelUnknown: unknown}
			c := readFixtureClient(t, f)
			stream, id, err := c.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, name[:8], &readObserverFixture{}, readFixtureOptions())
			if err != nil {
				t.Fatal(err)
			}
			_ = stream.Close()
			f.mu.Lock()
			f.state = "RUNNING"
			f.mu.Unlock()
			state, err := c.CancelRead(context.Background(), id, readFixtureOptions())
			if unknown {
				if state != ReadStateIndeterminate || err == nil {
					t.Fatalf("unknown: %s %v", state, err)
				}
			} else if state != ReadStateStopped || err != nil {
				t.Fatalf("stopped: %s %v", state, err)
			}
			if f.insert != 1 || f.cancelCount != 1 {
				t.Fatalf("redispatch or missing cancel: %+v", f)
			}
			id.Location = "EU"
			if state, err = c.ReadStatus(context.Background(), id); state != ReadStateIndeterminate || !errors.Is(err, ErrReadIdentity) {
				t.Fatal("foreign identity accepted")
			}
		})
	}
}

func TestReadParameterExactness(t *testing.T) {
	for _, value := range []any{nil, uint64(1), []int{1}, time.Unix(1, 1), ReadParameter{Type: "NUMERIC", Value: "0.0000000001"}, ReadParameter{Type: "BIGNUMERIC", Value: "1e100"}, "\xff"} {
		if _, err := readParameters([]any{value}); !errors.Is(err, ErrReadParameter) {
			t.Fatalf("accepted %#v: %v", value, err)
		}
	}
	if _, err := readParameters([]any{ReadParameter{Name: "x", Type: "STRING", Value: "x"}, ReadParameter{Name: "X", Type: "STRING", Value: "y"}}); err == nil {
		t.Fatal("duplicate parameter")
	}
	if _, err := readColumns(bq.Schema{{Name: "record", Type: bq.RecordFieldType}}); err == nil {
		t.Fatal("record accepted")
	}
	c := readFixtureClient(t, &readProtocolFixture{})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.readClient(context.Background()); !errors.Is(err, ErrReadClosed) {
		t.Fatal("closed client recreated")
	}
}

func TestReadTemporalParameters(t *testing.T) {
	timestamp := time.Date(2026, 9, 7, 12, 13, 14, 123456000, time.FixedZone("synthetic", -3*60*60))
	date := civil.Date{Year: 2026, Month: 9, Day: 7}
	clock := civil.Time{Hour: 12, Minute: 13, Second: 14, Nanosecond: 123456000}
	for _, value := range []any{timestamp, date, clock, civil.DateTime{Date: date, Time: clock}} {
		converted, err := scalarReadParameter(value)
		if err != nil || converted != value {
			t.Fatalf("temporal value changed: %#v %#v %v", value, converted, err)
		}
	}
	for _, value := range []any{civil.Date{Year: 0, Month: 1, Day: 1}, civil.DateTime{Date: date, Time: civil.Time{Nanosecond: 1}}, civil.Time{Nanosecond: 1}, ReadParameter{Type: "DATE", Value: timestamp}, ReadParameter{Type: "TIMESTAMP", Value: date}} {
		if _, err := readParameters([]any{value}); !errors.Is(err, ErrReadParameter) {
			t.Fatalf("inexact temporal accepted: %#v", value)
		}
	}
}

func TestReadTemporalProtocol(t *testing.T) {
	f := &readProtocolFixture{state: "DONE", temporal: true}
	c := readFixtureClient(t, f)
	stream, _, err := c.OpenRead(context.Background(), &query.Query{Query: "SELECT timestamp_value,date_value,time_value,datetime_value"}, "temporal", &readObserverFixture{}, readFixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Next() {
		t.Fatal(stream.Err())
	}
	values, err := stream.Values()
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != time.UnixMicro(1788783194123456).UTC() || values[1] != (civil.Date{Year: 2026, Month: 9, Day: 7}) || values[2] != (civil.Time{Hour: 12, Minute: 13, Second: 14, Nanosecond: 123456000}) || values[3] != (civil.DateTime{Date: civil.Date{Year: 2026, Month: 9, Day: 7}, Time: civil.Time{Hour: 12, Minute: 13, Second: 14, Nanosecond: 123456000}}) {
		t.Fatalf("temporal value changed: %#v", values)
	}
}
