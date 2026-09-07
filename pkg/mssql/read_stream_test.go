package mssql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/golang-sql/civil"
	mssqldriver "github.com/microsoft/go-mssqldb"
)

type readObserverFixture struct {
	deny          bool
	dispatch, ack int
	entered       chan ReadIdentity
}

func (o *readObserverFixture) OnDispatch(_ context.Context, id ReadIdentity) error {
	o.dispatch++
	if o.entered != nil {
		o.entered <- id
	}
	if o.deny {
		return errors.New("denied")
	}
	return nil
}

func (o *readObserverFixture) OnAcknowledged(context.Context, ReadIdentity) error {
	o.ack++
	return nil
}

func fixtureOptions() ReadOptions {
	return ReadOptions{Timeout: time.Second, CancelTimeout: time.Second}
}

func fixtureReadDB(t *testing.T, starts ...chan struct{}) (*DB, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	t.Helper()
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		err := sqlmock.QueryMatcherRegexp.Match(expected, actual)
		if err == nil && actual == "SELECT delayed" && len(starts) > 0 {
			close(starts[0])
		}
		return err
	})
	pool, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatal(err)
	}
	control, controlMock, err := sqlmock.New(sqlmock.ValueConverterOption(readFixtureConverter{}))
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{config: &Config{Host: "synthetic.invalid", Port: 1433, Database: "analytics"}, readDB: pool, readControl: control, activeReads: map[string]*activeRead{}}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		if err := controlMock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = pool.Close()
		_ = control.Close()
	})
	return db, mock, controlMock
}

func fixtureReadIdentity() ReadIdentity {
	return ReadIdentity{SessionID: 57, LoginTime: time.Date(2026, 9, 7, 12, 13, 14, 0, time.UTC), Server: "synthetic-server", Account: "synthetic-reader", Database: "analytics", AttemptTag: "attempt"}
}

func expectReadSession(mock sqlmock.Sqlmock) {
	id := fixtureReadIdentity()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(readIdentitySQL)).WillReturnRows(sqlmock.NewRows([]string{"spid", "login_time", "server", "account", "database"}).AddRow(id.SessionID, id.LoginTime, id.Server, id.Account, id.Database))
	mock.ExpectExec(regexp.QuoteMeta(readTagSQL)).WithArgs([]byte(id.AttemptTag)).WillReturnResult(sqlmock.NewResult(0, 0))
}

func TestReadStreamProtocol(t *testing.T) {
	t.Run("pre-dispatch denied", func(t *testing.T) {
		db, mock, _ := fixtureReadDB(t)
		expectReadSession(mock)
		mock.ExpectRollback()
		o := &readObserverFixture{deny: true}
		stream, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT 1"}, "attempt", o, fixtureOptions())
		if err == nil || stream != nil || o.dispatch != 1 || o.ack != 0 {
			t.Fatalf("denial: %v", err)
		}
	})
	t.Run("same-session verifier and exact bindings", func(t *testing.T) {
		db, mock, _ := fixtureReadDB(t)
		expectReadSession(mock)
		mock.ExpectQuery("SELECT DB_NAME").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("analytics"))
		columns := []*sqlmock.Column{sqlmock.NewColumn("n").OfType("BIGINT", int64(0)).Nullable(false), sqlmock.NewColumn("d").OfType("DECIMAL", []byte{}).WithPrecisionAndScale(29, 9), sqlmock.NewColumn("b").OfType("VARBINARY", []byte{}), sqlmock.NewColumn("t").OfType("DATETIMEOFFSET", time.Time{}), sqlmock.NewColumn("z").OfType("NVARCHAR", "").Nullable(true)}
		timestamp := time.Date(2026, 9, 7, 12, 13, 14, 123456700, time.FixedZone("synthetic", -3*3600))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @n, @text")).WithArgs(sql.Named("n", int64(9007199254740993)), sql.Named("text", "not SQL; literal")).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(columns...).AddRow(int64(9007199254740993), []byte("12345678901234567890.123456789"), []byte{0, 255}, timestamp, nil).AddRow(int64(2), []byte("0.000000001"), []byte{3}, timestamp, nil))
		mock.ExpectRollback()
		verify := func(ctx context.Context, session ReadSession) error {
			r, err := session.Query(ctx, &query.Query{Query: "SELECT DB_NAME()"})
			if err != nil {
				return err
			}
			defer r.Close()
			if !r.Next() {
				return errors.New("no database")
			}
			values, err := r.Values()
			if err != nil {
				return err
			}
			if values[0] != "analytics" {
				return errors.New("database mismatch")
			}
			return nil
		}
		o := &readObserverFixture{}
		stream, id, err := db.OpenReadVerified(context.Background(), &query.Query{Query: "SELECT @n, @text", Args: []any{sql.Named("n", int64(9007199254740993)), sql.Named("text", "not SQL; literal")}}, "attempt", o, fixtureOptions(), verify)
		if err != nil {
			t.Fatal(err)
		}
		if id != fixtureReadIdentity() || o.ack != 1 || len(stream.Columns()) != 5 || stream.Columns()[1].Precision != 29 {
			t.Fatal("identity/schema lost")
		}
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		values, err := stream.Values()
		if err != nil {
			t.Fatal(err)
		}
		if values[0] != int64(9007199254740993) || string(values[1].([]byte)) != "12345678901234567890.123456789" || string(values[2].([]byte)) != "\x00\xff" || values[3] != timestamp || values[4] != nil {
			t.Fatalf("native values: %#v", values)
		}
		if !stream.Next() {
			t.Fatal(stream.Err())
		}
		if string(values[1].([]byte)) != "12345678901234567890.123456789" {
			t.Fatal("prior value mutated")
		}
		if stream.Next() || stream.Err() != nil {
			t.Fatal("EOF lost")
		}
		if err = stream.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("empty result schema", func(t *testing.T) {
		db, mock, _ := fixtureReadDB(t)
		expectReadSession(mock)
		mock.ExpectQuery("SELECT empty").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("empty").OfType("INT", int64(0))))
		mock.ExpectRollback()
		stream, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT empty"}, "attempt", &readObserverFixture{}, fixtureOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		if len(stream.Columns()) != 1 || stream.Next() || stream.Err() != nil {
			t.Fatal("empty schema missing")
		}
	})
	t.Run("cancel original in-flight connection", func(t *testing.T) {
		started := make(chan struct{})
		db, mock, _ := fixtureReadDB(t, started)
		expectReadSession(mock)
		mock.ExpectQuery("SELECT delayed").WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(1))
		mock.ExpectRollback()
		entered := make(chan ReadIdentity, 1)
		returned := make(chan error, 1)
		go func() {
			_, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT delayed"}, "attempt", &readObserverFixture{entered: entered}, fixtureOptions())
			returned <- err
		}()
		id := <-entered
		<-started
		state, err := db.CancelRead(context.Background(), id, fixtureOptions())
		if err != nil || state != ReadStateStopped {
			t.Fatalf("cancel: %s %v", state, err)
		}
		if err = <-returned; err == nil {
			t.Fatal("cancelled query succeeded")
		}
		fresh, _, _ := fixtureReadDB(t)
		if state, err = fresh.CancelRead(context.Background(), id, fixtureOptions()); state != ReadStateIndeterminate || !errors.Is(err, ErrReadNotOwned) {
			t.Fatal("fresh client claimed connection ownership")
		}
	})
	t.Run("native status and absent identity", func(t *testing.T) {
		db, _, control := fixtureReadDB(t)
		id := fixtureReadIdentity()
		for _, count := range []int{1, 0} {
			control.ExpectQuery("SELECT CAST").WillReturnRows(sqlmock.NewRows([]string{"server"}).AddRow(id.Server))
			control.ExpectQuery(regexp.QuoteMeta(readStatusSQL)).WillReturnRows(sqlmock.NewRows([]string{"count", "running"}).AddRow(count, 0))
			state, err := db.ReadStatus(context.Background(), id, fixtureOptions())
			if err != nil {
				t.Fatal(err)
			}
			if count == 1 && state != ReadStateStopped || count == 0 && state != ReadStateIndeterminate {
				t.Fatal("native status fabricated")
			}
		}
	})
}

func TestReadArguments(t *testing.T) {
	for _, value := range []any{big.NewRat(1, 10), int8(-1), time.Unix(1, 1), []int{1}, "\xff", sql.Out{Dest: new(int)}} {
		if _, err := readArguments([]any{value}); !errors.Is(err, ErrReadParameter) {
			t.Fatalf("accepted unsupported/inexact value %#v", value)
		}
	}
	for _, value := range []any{int64(9007199254740993), []byte{0, 255}, civil.Date{Year: 2026, Month: 9, Day: 7}, civil.Time{Hour: 12, Nanosecond: 100}, civil.DateTime{Date: civil.Date{Year: 2026, Month: 9, Day: 7}}} {
		if _, err := readArguments([]any{value}); err != nil {
			t.Fatalf("rejected exact value %#v: %v", value, err)
		}
	}
	db, _, _ := fixtureReadDB(t)
	if err := db.initializeReadPools(ReadOptions{RequireTLS: true}); !errors.Is(err, ErrReadIdentity) {
		t.Fatal("implicit insecure TLS accepted")
	}
	db.readMu.Lock()
	db.readClosed = true
	db.readMu.Unlock()
	if err := db.initializeReadPools(ReadOptions{}); !errors.Is(err, ErrReadClosed) {
		t.Fatal("closed pool recreated")
	}
}

type readFixtureConverter struct{}

func (readFixtureConverter) ConvertValue(value any) (driver.Value, error) {
	if v, ok := value.(mssqldriver.DateTime1); ok {
		return time.Time(v), nil
	}
	return driver.DefaultParameterConverter.ConvertValue(value)
}

func TestReadNativeParameterCompatibility(t *testing.T) {
	native := new(mssqldriver.Conn)
	values := []any{int64(9007199254740993), []byte{0, 255}, "synthetic", time.Date(2026, 9, 7, 12, 13, 14, 123456700, time.FixedZone("synthetic", -3*3600)), civil.Date{Year: 2026, Month: 9, Day: 7}, civil.Time{Hour: 12, Nanosecond: 100}}
	for i, value := range values {
		converted, err := readArgument(value)
		if err != nil {
			t.Fatal(err)
		}
		argument := driver.NamedValue{Ordinal: i + 1, Value: converted}
		if err = native.CheckNamedValue(&argument); err != nil {
			t.Fatalf("Microsoft SDK rejected %T: %v", converted, err)
		}
	}
}

func TestReadFailureCleanupEvidence(t *testing.T) {
	queryError := errors.New("synthetic query failure")
	for _, fails := range []bool{false, true} {
		name := "acknowledged rollback"
		if fails {
			name = "unresolved rollback"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, _ := fixtureReadDB(t)
			expectReadSession(mock)
			mock.ExpectQuery("SELECT failure").WillReturnError(queryError)
			rollback := mock.ExpectRollback()
			if fails {
				rollback.WillReturnError(errors.New("synthetic transport loss"))
			}
			_, _, err := db.OpenRead(context.Background(), &query.Query{Query: "SELECT failure"}, "attempt", &readObserverFixture{}, fixtureOptions())
			var failure *ReadFailure
			if !errors.As(err, &failure) || !errors.Is(err, queryError) || failure.Stopped == fails {
				t.Fatalf("cleanup evidence: %#v %v", failure, err)
			}
		})
	}
}
