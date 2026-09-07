//go:build cgo && (darwin || linux)

package sqlparser

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestRustSQLParserSmoke(t *testing.T) {
	t.Parallel()

	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, parser.Close())
	})

	lineage, err := parser.ColumnLineage(
		"SELECT IF(col1 IS NOT NULL, 1, 0) AS x FROM t",
		"bigquery",
		Schema{"t": {"col1": "STRING"}},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		[]ColumnLineage{
			{
				Name: "x",
				Upstream: []UpstreamColumn{
					{Column: "col1", Table: "t"},
				},
				Type: "INT",
			},
		},
		lineage.Columns,
	)
	require.Empty(t, lineage.Errors)

	tables, err := parser.UsedTables(
		"WITH base AS (SELECT * FROM raw.my_cte) SELECT * FROM base",
		"bigquery",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"raw.my_cte"}, tables)
}

func TestRustSQLParserInspectRead(t *testing.T) {
	parser, err := NewRustSQLParserWithConfig(false, 32768)
	require.NoError(t, err)
	require.NoError(t, parser.Start())

	inspection, err := parser.InspectRead("WITH recent AS (SELECT id, amount FROM analytics.facts WHERE id = ?) SELECT id, SUM(amount) AS total FROM recent GROUP BY id", "mysql", 256, 32)
	require.NoError(t, err)
	require.Equal(t, []string{"analytics.facts"}, inspection.Tables)
	require.Contains(t, inspection.Functions, "sum")
	require.Equal(t, []string{"id", "total"}, inspection.Outputs)
	require.Equal(t, 1, inspection.Parameters)
	require.Positive(t, inspection.Nodes)

	for _, statement := range []string{
		"SELECT 1; SELECT 2",
		"SELECT 1 INTO OUTFILE '/tmp/result'",
		"SELECT @secret",
		"SELECT * FROM analytics.facts FOR UPDATE",
		"SELECT /*!50000 SLEEP(10) */ 1",
		"SELECT /*+ MAX_EXECUTION_TIME(1) */ 1",
	} {
		_, err := parser.InspectRead(statement, "mysql", 256, 32)
		require.Error(t, err, statement)
	}
	_, err = parser.InspectRead("SELECT id FROM analytics.facts", "mysql", 1, 32)
	require.Error(t, err)
}

func TestRustSQLParserInspectReadRejectsRecursiveInputBeforeNativeParser(t *testing.T) {
	if os.Getenv("BRUIN_INSPECT_RECURSIVE_HELPER") == "1" {
		parser, err := NewRustSQLParserWithConfig(false, 32768)
		require.NoError(t, err)
		statement := "SELECT " + strings.Repeat("CASE WHEN TRUE THEN ", 1000) + "1" + strings.Repeat(" ELSE 0 END", 1000)
		_, err = parser.InspectRead(statement, "mysql", 256, 32)
		require.Error(t, err)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRustSQLParserInspectReadRejectsRecursiveInputBeforeNativeParser$")
	cmd.Env = append(os.Environ(), "BRUIN_INSPECT_RECURSIVE_HELPER=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestRustSQLParserInspectReadDialects(t *testing.T) {
	parser, err := NewRustSQLParserWithConfig(false, 32768)
	require.NoError(t, err)
	tests := []struct{ dialect, sql, table string }{
		{"mysql", "SELECT id FROM warehouse.facts WHERE id = ?", "warehouse.facts"},
		{"tsql", "SELECT id FROM dbo.facts WHERE id = @p1", "dbo.facts"},
		{"bigquery", "SELECT id FROM project.dataset.facts WHERE id = @id", "project.dataset.facts"},
		{"snowflake", "SELECT id FROM database.schema.facts WHERE id = ?", "database.schema.facts"},
		{"databricks", "SELECT id FROM catalog.schema.facts WHERE id = :id", "catalog.schema.facts"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			inspection, err := parser.InspectRead(test.sql, test.dialect, 128, 24)
			require.NoError(t, err)
			require.Equal(t, []string{test.table}, inspection.Tables)
			require.Equal(t, 1, inspection.Parameters, "native parameter count")
			require.Equal(t, []ReadColumn{{Name: "id"}}, inspection.Columns, "parameters are not column dependencies")
			_, err = parser.InspectRead(test.sql+" trailing", test.dialect, 128, 24)
			require.Error(t, err, "parser must consume the complete input")
		})
	}
}

func TestRustSQLParserInspectReadAtParameters(t *testing.T) {
	parser, err := NewRustSQLParserWithConfig(false, 32768)
	require.NoError(t, err)
	for _, dialect := range []string{"tsql", "bigquery"} {
		t.Run(dialect, func(t *testing.T) {
			for _, test := range []struct {
				predicate string
				count     int
			}{
				{"id > @p1", 1},
				{"id > @p1 AND id < @p1", 1},
				{"id > @p2 AND id < @p1", 2},
				{"id > @p1 AND id IN (SELECT id FROM dbo.facts WHERE id < @p1)", 1},
				{"id = '@p1'", 0},
			} {
				inspection, err := parser.InspectRead("SELECT id FROM dbo.facts WHERE "+test.predicate, dialect, 128, 24)
				require.NoError(t, err, test.predicate)
				require.Equal(t, test.count, inspection.Parameters, test.predicate)
				require.Equal(t, []ReadColumn{{Name: "id"}}, inspection.Columns, test.predicate)
			}
			for _, marker := range []string{"@@version", `@"p1"`, "@'p1'"} {
				_, err := parser.InspectRead("SELECT id FROM dbo.facts WHERE id = "+marker, dialect, 128, 24)
				require.Error(t, err, marker)
			}
		})
	}
	for _, test := range []struct {
		dialect, sql string
		column       ReadColumn
	}{
		{"tsql", "SELECT [@p1] AS value FROM dbo.facts", ReadColumn{Name: "@p1"}},
		{"tsql", "SELECT f.[@p1] AS value FROM dbo.facts f", ReadColumn{Table: "f", Name: "@p1"}},
		{"bigquery", "SELECT `@p1` AS value FROM dbo.facts", ReadColumn{Name: "@p1"}},
		{"bigquery", "SELECT f.`@p1` AS value FROM dbo.facts f", ReadColumn{Table: "f", Name: "@p1"}},
	} {
		inspection, err := parser.InspectRead(test.sql, test.dialect, 128, 24)
		require.NoError(t, err, test.sql)
		require.Zero(t, inspection.Parameters, test.sql)
		require.Equal(t, []ReadColumn{test.column}, inspection.Columns, test.sql)
	}
}

func FuzzRustSQLParserInspectRead(f *testing.F) {
	for _, seed := range []string{
		"SELECT id FROM analytics.facts WHERE id = ?",
		"SELECT 1; DELETE FROM analytics.facts",
		"SELECT /*!50000 SLEEP(10) */ 1",
		"SELECT " + strings.Repeat("(", 80) + "1" + strings.Repeat(")", 80),
		"SELECT @secret",
	} {
		f.Add(seed)
	}
	parser, err := NewRustSQLParserWithConfig(false, 32768)
	require.NoError(f, err)
	f.Fuzz(func(t *testing.T, sql string) {
		inspection, err := parser.InspectRead(sql, "mysql", 256, 32)
		if err == nil {
			require.NotEmpty(t, inspection.Outputs)
			require.LessOrEqual(t, inspection.Nodes, 256)
			require.LessOrEqual(t, inspection.Depth, 32)
		}
	})
}

func TestRustSQLParser_HoistDeclares(t *testing.T) {
	t.Parallel()

	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Start())

	t.Run("no declare is a no-op", func(t *testing.T) {
		t.Parallel()
		got, err := parser.HoistDeclares("SELECT 1; SELECT 2;", pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1; SELECT 2;", got)
	})

	t.Run("already-ordered declare returns input verbatim", func(t *testing.T) {
		t.Parallel()
		in := "DECLARE x INT64;\nSELECT 1;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("declare after non-declare gets hoisted with original text preserved", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\nDECLARE y INT64;\nSELECT 1;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		// Each statement's original text is preserved verbatim — only order
		// and the ';\n' separator are rewritten.
		require.Equal(t, "DECLARE y INT64;\nSET x = 1;\nSELECT 1;", got)
	})

	t.Run("declare keyword inside string literal does not trigger reordering", func(t *testing.T) {
		t.Parallel()
		in := "SELECT 'declare bankruptcy' AS msg;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("semicolon inside string literal does not split", func(t *testing.T) {
		t.Parallel()
		in := "SET separator = ';';\nDECLARE y INT64;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "DECLARE y INT64;\nSET separator = ';';", got)
	})

	t.Run("declare inside BEGIN..END block is not hoisted", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEND;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("CASE..END inside BEGIN body does not leak DECLAREs", func(t *testing.T) {
		t.Parallel()
		// CASE shares its closing END token with BEGIN. Without per-construct
		// depth tracking, the CASE's END would prematurely close the BEGIN
		// block and the inner DECLARE would be hoisted out, breaking the
		// stored-procedure body.
		in := "SET x = 1;\nBEGIN\n  SELECT CASE WHEN x>0 THEN 'a' ELSE 'b' END;\n  DECLARE y INT64;\nEND;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("leading comment is preserved with its statement", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\n-- setup\nDECLARE y INT64;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "-- setup\nDECLARE y INT64;\nSET x = 1;", got)
	})

	t.Run("array type syntax preserved verbatim", func(t *testing.T) {
		t.Parallel()
		// `array<STRING>` lower-case casing must survive — we slice the
		// original text rather than regenerating from the AST.
		in := "SET x = 1;\nDECLARE distinct_keys array<STRING>;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "DECLARE distinct_keys array<STRING>;\nSET x = 1;", got)
	})

	t.Run("BEGIN TRANSACTION does not swallow following semicolons", func(t *testing.T) {
		t.Parallel()
		// `BEGIN TRANSACTION` emits TokenType::Begin without a matching
		// END (COMMIT/ROLLBACK don't produce TokenType::End). If we
		// blindly tracked it as a block, every ';' after it would be
		// classified as nested and the script would collapse into one
		// non-DECLARE slice.
		in := "DECLARE distinct_keys array<STRING>;\n" +
			"BEGIN TRANSACTION;\n" +
			"SELECT 1;\n" +
			"COMMIT TRANSACTION;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		// Already-ordered: input returned verbatim.
		require.Equal(t, in, got)
	})

	t.Run("hoist past BEGIN TRANSACTION", func(t *testing.T) {
		t.Parallel()
		// A DECLARE appearing after a BEGIN TRANSACTION must still be
		// recognized as top-level and hoisted to the front. Without the
		// BEGIN TRANSACTION lookahead, it would be wrongly treated as
		// nested inside the transaction "block" and skipped.
		in := "BEGIN TRANSACTION;\nSET x = 1;\nDECLARE y INT64;\nCOMMIT TRANSACTION;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(
			t,
			"DECLARE y INT64;\nBEGIN TRANSACTION;\nSET x = 1;\nCOMMIT TRANSACTION;",
			got,
		)
	})

	t.Run("unmapped asset type returns error and input unchanged", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\nDECLARE y INT64;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypePython)
		require.Error(t, err)
		require.Equal(t, in, got)
	})
}

func TestRustSQLParser_HoistDeclaresList(t *testing.T) {
	t.Parallel()

	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Start())

	t.Run("no declare is a no-op", func(t *testing.T) {
		t.Parallel()
		in := []string{"SELECT 1", "SELECT 2"}
		got, err := parser.HoistDeclaresList(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("declare after non-declare gets hoisted while preserving text", func(t *testing.T) {
		t.Parallel()
		in := []string{"SET x = 1", "DECLARE y INT64", "SELECT 1"}
		got, err := parser.HoistDeclaresList(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, []string{"DECLARE y INT64", "SET x = 1", "SELECT 1"}, got)
	})
}
