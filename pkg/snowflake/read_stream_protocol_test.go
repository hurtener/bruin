package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
)

type recordedSnowflake struct {
	mu         sync.Mutex
	requests   []map[string]any
	requestIDs []string
	retryUser  bool
}

func (f *recordedSnowflake) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if strings.Contains(r.URL.Path, "login-request") {
		_, _ = io.WriteString(w, `{"success":true,"data":{"token":"synthetic-session","masterToken":"synthetic-master","validityInSeconds":3600,"masterValidityInSeconds":3600,"sessionId":42,"parameters":[],"sessionInfo":{"databaseName":"WAREHOUSE","schemaName":"PUBLIC","warehouseName":"COMPUTE","roleName":"READER"}}}`)
		return
	}
	if strings.Contains(r.URL.Path, "session") && strings.Contains(r.URL.Path, "delete") {
		_, _ = io.WriteString(w, `{"success":true}`)
		return
	}
	data, _ := io.ReadAll(r.Body)
	var request map[string]any
	_ = json.Unmarshal(data, &request)
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.requestIDs = append(f.requestIDs, r.URL.Query().Get("requestId"))
	sqlText, _ := request["sqlText"].(string)
	if strings.Contains(sqlText, "FROM facts") && !strings.Contains(sqlText, "WHERE FALSE") && !f.retryUser {
		f.retryUser = true
		f.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"success":false,"message":"retry"}`)
		return
	}
	f.mu.Unlock()
	response := map[string]any{"success": true, "data": map[string]any{"queryId": "qid-control", "queryResultFormat": "json", "rowtype": []any{}, "rowset": []any{}, "total": 0, "returned": 0}}
	result := response["data"].(map[string]any)
	switch {
	case sqlText == "SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SESSION()":
		result["queryId"] = "qid-preflight"
		result["rowtype"] = []any{rowType("CURRENT_ACCOUNT()", "text", 0, 0), rowType("CURRENT_DATABASE()", "text", 0, 0), rowType("CURRENT_SESSION()", "fixed", 38, 0)}
		result["rowset"] = []any{[]any{"ORG.ACCOUNT", "WAREHOUSE", "42"}}
		result["total"], result["returned"] = 1, 1
	case strings.Contains(sqlText, "WHERE FALSE"):
		result["queryId"] = "01b12345-0000-0000-0000-000000000002"
		result["rowtype"] = []any{rowType("AMOUNT", "fixed", 38, 3)}
	case strings.Contains(sqlText, "FROM facts"):
		result["queryId"] = "01b12345-0000-0000-0000-000000000001"
		result["rowtype"] = []any{rowType("AMOUNT", "fixed", 38, 3), rowType("PAYLOAD", "binary", 0, 0)}
		result["rowset"] = []any{[]any{"9007199254740993.125", "00FF"}}
		result["total"], result["returned"] = 1, 1
	case sqlText == "SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE()":
		result["rowtype"] = []any{rowType("CURRENT_ACCOUNT()", "text", 0, 0), rowType("CURRENT_DATABASE()", "text", 0, 0)}
		result["rowset"] = []any{[]any{"ORG.ACCOUNT", "WAREHOUSE"}}
		result["total"], result["returned"] = 1, 1
	case strings.Contains(sqlText, "QUERY_HISTORY_BY_SESSION"):
		result["rowtype"] = []any{rowType("EXECUTION_STATUS", "text", 0, 0), rowType("QUERY_TAG", "text", 0, 0)}
		result["rowset"] = []any{[]any{"RUNNING", "cw:protocol-1"}}
		result["total"], result["returned"] = 1, 1
	case strings.Contains(sqlText, "SYSTEM$CANCEL_QUERY"):
		result["rowtype"] = []any{rowType("SYSTEM$CANCEL_QUERY", "text", 0, 0)}
		result["rowset"] = []any{[]any{"query cancelled"}}
		result["total"], result["returned"] = 1, 1
	}
	_ = json.NewEncoder(w).Encode(response)
}

func rowType(name, typ string, precision, scale int64) map[string]any {
	return map[string]any{"name": name, "type": typ, "nullable": false, "length": 64, "byteLength": 64, "precision": precision, "scale": scale}
}

func TestOpenReadRecordedSnowflakeSDKProtocol(t *testing.T) {
	fixture := &recordedSnowflake{}
	server := httptest.NewTLSServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	cfg := gosnowflake.Config{Account: "account", User: "reader", Password: "synthetic", Database: "WAREHOUSE", Schema: "PUBLIC", Warehouse: "COMPUTE", Host: u.Hostname(), Port: mustPort(t, u), Protocol: "https", Transporter: server.Client().Transport, DisableOCSPChecks: true, ValidateDefaultParameters: gosnowflake.ConfigBoolFalse, MaxRetryCount: 1}
	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, cfg)
	native := sql.OpenDB(connector)
	t.Cleanup(func() { _ = native.Close() })
	db := &DB{conn: sqlx.NewDb(native, "snowflake")}
	observer := &recordingReadObserver{}
	stream, identity, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT amount, payload FROM facts WHERE id = ?", Args: []any{int64(7)}}, "protocol-1", observer)
	require.NoError(t, err)
	require.Equal(t, identity, observer.acknowledged)
	require.Empty(t, observer.dispatched.QueryID)
	if !stream.Next() {
		fixture.mu.Lock()
		t.Fatalf("recorded result had no row: err=%v requests=%#v", stream.Err(), fixture.requests)
	}
	values, err := stream.Values()
	require.NoError(t, err)
	decimal := values[0].(big.Float)
	require.Equal(t, "9007199254740993.125", decimal.Text('f', 3))
	require.Equal(t, []byte{0, 255}, values[1])
	require.NoError(t, stream.Close())
	state, err := db.ReadStatus(context.Background(), identity)
	require.NoError(t, err)
	require.Equal(t, ReadStateRunning, state)
	require.NoError(t, db.CancelRead(context.Background(), identity))
	empty, emptyIdentity, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT amount FROM facts WHERE FALSE"}, "protocol-empty", &recordingReadObserver{})
	require.NoError(t, err)
	require.NotEmpty(t, emptyIdentity.QueryID)
	require.Len(t, empty.Columns(), 1)
	require.True(t, empty.Columns()[0].DecimalKnown)
	require.False(t, empty.Next())
	require.NoError(t, empty.Err())
	require.NoError(t, empty.Close())

	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var userRequests []int
	var history, cancel bool
	for i, request := range fixture.requests {
		sqlText, _ := request["sqlText"].(string)
		if strings.Contains(sqlText, "FROM facts") && !strings.Contains(sqlText, "WHERE FALSE") {
			userRequests = append(userRequests, i)
			bindings := request["bindings"].(map[string]any)
			require.Equal(t, "7", bindings["1"].(map[string]any)["value"])
			require.Equal(t, identity.QueryTag, request["parameters"].(map[string]any)["QUERY_TAG"])
		}
		if strings.Contains(sqlText, "QUERY_HISTORY_BY_SESSION") {
			bindings := request["bindings"].(map[string]any)
			require.Equal(t, "42", bindings["1"].(map[string]any)["value"])
			require.Equal(t, identity.QueryID, bindings["2"].(map[string]any)["value"])
			history = true
		}
		if strings.Contains(sqlText, "SYSTEM$CANCEL_QUERY") {
			bindings := request["bindings"].(map[string]any)
			require.Equal(t, identity.QueryID, bindings["1"].(map[string]any)["value"])
			cancel = true
		}
	}
	require.Len(t, userRequests, 2, "recorded transport retry was not exercised")
	require.Equal(t, fixture.requestIDs[userRequests[0]], fixture.requestIDs[userRequests[1]], "transport retry changed deterministic request identity")
	require.True(t, history && cancel, "recorded status/cancel protocol was not exercised")
}

func mustPort(t *testing.T, u *url.URL) int {
	t.Helper()
	var port int
	_, err := fmt.Sscanf(u.Port(), "%d", &port)
	require.NoError(t, err)
	return port
}
