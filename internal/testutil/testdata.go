package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// RunSqlScript loads the test DDL and data into a PostgreSQL container
func RunSqlScript(ctx context.Context, connStr string, scriptFile string) error {
	// Read the test data SQL file
	sqlFile := filepath.Join(scriptFile)
	sqlContent, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("failed to read test data file: %w", err)
	}

	// Connect to the database
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Execute the SQL
	_, err = pool.Exec(ctx, string(sqlContent))
	if err != nil {
		return fmt.Errorf("failed to execute test data SQL: %w", err)
	}

	log.Info().Msg(fmt.Sprintf("Script %s executed successfully in connection %s", scriptFile, connStr))
	return nil
}

// VerifyTestData verifies that the test data was loaded correctly
func VerifyTestData(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Check if tables exist
	tables := []string{
		"public.users",
		"public.products",
		"public.orders",
		"analytics.page_views",
		"public.complex_data",
	}

	for _, table := range tables {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		err := pool.QueryRow(ctx, query).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to verify table %s: %w", table, err)
		}
		log.Info().Str("table", table).Int("row_count", count).Msg("Table verified")
	}

	return nil
}

// GetTestDataStats returns statistics about the loaded test data
func GetTestDataStats(ctx context.Context, connStr string) (map[string]int, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	stats := make(map[string]int)
	tables := []string{
		"public.users",
		"public.products",
		"public.orders",
		"analytics.page_views",
		"public.complex_data",
	}

	for _, table := range tables {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		err := pool.QueryRow(ctx, query).Scan(&count)
		if err != nil {
			return nil, fmt.Errorf("failed to get count for table %s: %w", table, err)
		}
		stats[table] = count
	}

	return stats, nil
}

// WaitForDatabaseReady waits for the database to be ready for connections
func WaitForDatabaseReady(ctx context.Context, connStr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		pool, err := pgxpool.New(ctx, connStr)
		if err == nil {
			pool.Close()
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			continue
		}
	}

	return fmt.Errorf("database not ready within %v", timeout)
}
