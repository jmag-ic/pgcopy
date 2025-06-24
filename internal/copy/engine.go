package copy

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"pgcopy/internal/db"
	"pgcopy/internal/schema"
)

// Engine represents the copy engine
type Engine struct {
	sourceConn *db.Connection
	targetConn *db.Connection
	stats      *Stats
}

// Stats represents copy statistics
type Stats struct {
	TablesProcessed int
	RowsCopied      int64
	Errors          []error
	StartTime       time.Time
	EndTime         time.Time
}

// NewEngine creates a new copy engine
func NewEngine(sourceURL, targetURL string) (*Engine, error) {
	ctx := context.Background()

	sourceConn, err := db.NewConnection(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	targetConn, err := db.NewConnection(ctx, targetURL)
	if err != nil {
		sourceConn.Close()
		return nil, fmt.Errorf("failed to connect to target database: %w", err)
	}

	return &Engine{
		sourceConn: sourceConn,
		targetConn: targetConn,
		stats: &Stats{
			StartTime: time.Now(),
		},
	}, nil
}

// Close closes the engine and all connections
func (e *Engine) Close() {
	if e.sourceConn != nil {
		e.sourceConn.Close()
	}
	if e.targetConn != nil {
		e.targetConn.Close()
	}
}

// Copy performs the copy operation
func (e *Engine) Copy(ctx context.Context, config *schema.Config) error {
	tables := config.GetAllTables()
	log.Info().Int("total_tables", len(tables)).Msg("Starting copy operation")

	// Process tables concurrently
	for _, table := range tables {

		if err := e.copyTable(ctx, table); err != nil {
			e.addError(err)
			log.Error().Err(err).Str("schema", table.Schema).Str("table", table.Table).Msg("Failed to copy table")
		} else {
			e.incrementTablesProcessed()
			log.Info().Str("schema", table.Schema).Str("table", table.Table).Msg("Table copied successfully")
		}
	}

	e.stats.EndTime = time.Now()

	e.printSummary()
	return nil
}

// DryRun shows what would be copied without executing
func (e *Engine) DryRun(ctx context.Context, config *schema.Config) error {
	tables := config.GetAllTables()

	log.Info().Int("total_tables", len(tables)).Msg("DRY RUN - Tables that would be copied:")

	for _, table := range tables {
		log.Info().
			Str("schema", table.Schema).
			Str("table", table.Table).
			Strs("ignore", table.Ignore).
			Str("filter", table.Filter).
			Bool("truncate", table.Truncate).
			Msg("Table configuration")
	}

	return nil
}

// copyTable copies a single table using COPY protocol
func (e *Engine) copyTable(ctx context.Context, table schema.TableInfo) error {
	// Truncate target table if requested
	if table.Truncate {
		if err := e.truncateTable(ctx, table); err != nil {
			return fmt.Errorf("failed to truncate table %s.%s: %w", table.Schema, table.Table, err)
		}
		log.Info().Str("schema", table.Schema).Str("table", table.Table).Msg("Table truncated before copy")
	}

	// Build column list
	columns, err := e.getTableColumns(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get table columns: %w", err)
	}

	// Build COPY commands
	sourceQuery, err := e.buildSourceCopyQuery(table, columns)
	if err != nil {
		return fmt.Errorf("failed to build source copy query: %w", err)
	}
	targetQuery, err := e.buildTargetCopyQuery(table, columns)
	if err != nil {
		return fmt.Errorf("failed to build target copy query: %w", err)
	}

	log.Debug().
		Str("schema", table.Schema).
		Str("table", table.Table).
		Str("source_query", sourceQuery).
		Str("target_query", targetQuery).
		Msg("Executing COPY")

	// Execute copy using native COPY protocol
	return e.executeCopyWithProtocol(ctx, sourceQuery, targetQuery)
}

// getTableColumns gets the columns for a table
func (e *Engine) getTableColumns(ctx context.Context, table schema.TableInfo) ([]string, error) {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2 
		ORDER BY ordinal_position
	`

	rows, err := e.sourceConn.GetPool().Query(ctx, query, table.Schema, table.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}

		// Skip ignored columns
		if !slices.Contains(table.Ignore, column) {
			columns = append(columns, column)
		}
	}

	return columns, rows.Err()
}

// buildSourceCopyQuery builds the source COPY query
func (e *Engine) buildSourceCopyQuery(table schema.TableInfo, columns []string) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("no columns to copy for table %s.%s", table.Schema, table.Table)
	}

	// Build column list with transformations
	var columnList []string
	for _, col := range columns {
		if transformation, exists := table.Transform[col]; exists {
			// Apply transformation
			transformedCol := e.expandTransformation(transformation, col)
			columnList = append(columnList, fmt.Sprintf("%s AS %s", transformedCol, col))
		} else {
			// Use column as-is
			columnList = append(columnList, col)
		}
	}

	query := fmt.Sprintf("COPY (SELECT %s FROM %s.%s) TO STDOUT",
		formatColumns(columnList), table.Schema, table.Table)

	if table.Filter != "" {
		query = fmt.Sprintf("COPY (SELECT %s FROM %s.%s WHERE %s) TO STDOUT",
			formatColumns(columnList), table.Schema, table.Table, table.Filter)
	}

	return query, nil
}

// expandTransformation expands built-in transformation functions or applies custom SQL
func (e *Engine) expandTransformation(transformation string, columnName string) string {
	switch transformation {
	case "hash":
		return fmt.Sprintf("encode(sha256(%s::text::bytea), 'hex')", columnName)
	case "redact":
		return "'***REDACTED***'"
	case "anonymize":
		return fmt.Sprintf("'anon-' || encode(sha256(%s::text::bytea), 'hex')", columnName)
	case "nullify":
		return "NULL"
	case "default":
		// Note: This is a simplified version. In a real implementation, you'd need to pass table info
		return fmt.Sprintf("COALESCE(%s, NULL)", columnName)
	default:
		// Assume it's a custom SQL expression, replace $1 with column name
		return strings.ReplaceAll(transformation, "$1", columnName)
	}
}

// buildTargetCopyQuery builds the target COPY query
func (e *Engine) buildTargetCopyQuery(table schema.TableInfo, columns []string) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("no columns to copy for table %s.%s", table.Schema, table.Table)
	}

	query := fmt.Sprintf("COPY %s.%s (%s) FROM STDIN", table.Schema, table.Table, formatColumns(columns))

	return query, nil
}

// truncateTable truncates the target table
func (e *Engine) truncateTable(ctx context.Context, table schema.TableInfo) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", table.Schema, table.Table)

	_, err := e.targetConn.GetPool().Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute truncate: %w", err)
	}

	return nil
}

// executeCopyWithProtocol executes the copy operation using native COPY protocol
func (e *Engine) executeCopyWithProtocol(ctx context.Context, sourceQuery, targetQuery string) error {
	// Get connections
	sourceConn, err := e.sourceConn.GetPool().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire source connection: %w", err)
	}
	defer sourceConn.Release()

	targetConn, err := e.targetConn.GetPool().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire target connection: %w", err)
	}
	defer targetConn.Release()

	// Create a pipe to stream data between source and target
	r, w := io.Pipe()

	// Start source COPY in a goroutine
	go func() {
		defer w.Close()
		_, err := sourceConn.Conn().PgConn().CopyTo(ctx, w, sourceQuery)
		if err != nil {
			w.CloseWithError(fmt.Errorf("source copy failed: %w", err))
		}
	}()

	// Execute target COPY
	commandTag, err := targetConn.Conn().PgConn().CopyFrom(ctx, r, targetQuery)
	if err != nil {
		return fmt.Errorf("target copy failed: %w", err)
	}

	rowsCopied := commandTag.RowsAffected()
	e.incrementRowsCopied(rowsCopied)
	log.Info().Int64("rows_copied", rowsCopied).Msg("Table copy completed")

	return nil
}

// formatColumns formats column names for SQL
func formatColumns(columns []string) string {
	if len(columns) == 0 {
		return ""
	}

	result := columns[0]
	for i := 1; i < len(columns); i++ {
		result += ", " + columns[i]
	}
	return result
}

// addError adds an error to the stats
func (e *Engine) addError(err error) {
	e.stats.Errors = append(e.stats.Errors, err)
}

// incrementTablesProcessed increments the tables processed counter
func (e *Engine) incrementTablesProcessed() {
	e.stats.TablesProcessed++
}

// incrementRowsCopied increments the rows copied counter
func (e *Engine) incrementRowsCopied(count int64) {
	e.stats.RowsCopied += count
}

// printSummary prints the copy summary
func (e *Engine) printSummary() {
	duration := e.stats.EndTime.Sub(e.stats.StartTime)

	log.Info().
		Int("tables_processed", e.stats.TablesProcessed).
		Int64("rows_copied", e.stats.RowsCopied).
		Int("errors", len(e.stats.Errors)).
		Dur("duration", duration).
		Msg("Copy operation completed")

	if len(e.stats.Errors) > 0 {
		log.Error().Int("error_count", len(e.stats.Errors)).Msg("Copy operation completed with errors")
		for i, err := range e.stats.Errors {
			log.Error().Err(err).Int("error_index", i).Msg("Copy error")
		}
	}
}
