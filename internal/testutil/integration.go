package testutil

import (
	"context"
	"testing"
	"time"
)

// TestEnvironment represents a test environment with source and target databases
type TestEnvironment struct {
	SourceDB *PostgresContainer
	TargetDB *PostgresContainer
}

// SetupTestEnvironment creates a test environment with two PostgreSQL containers
func SetupTestEnvironment(t *testing.T) *TestEnvironment {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start source database
	sourceConfig := PostgresConfig{
		User:     "source_user",
		Password: "source_pass",
		Database: "source_db",
		Port:     "5438",
	}
	sourceDB, err := StartPostgresContainer(ctx, sourceConfig)
	if err != nil {
		t.Fatalf("failed to start source database: %v", err)
	}

	// Start target database
	targetConfig := PostgresConfig{
		User:     "target_user",
		Password: "target_pass",
		Database: "target_db",
		Port:     "5439",
	}
	targetDB, err := StartPostgresContainer(ctx, targetConfig)
	if err != nil {
		sourceDB.Stop(ctx)
		t.Fatalf("failed to start target database: %v", err)
	}

	// Wait for both databases to be ready
	if err := sourceDB.WaitForReady(ctx, 30*time.Second); err != nil {
		sourceDB.Stop(ctx)
		targetDB.Stop(ctx)
		t.Fatalf("source database not ready: %v", err)
	}

	if err := targetDB.WaitForReady(ctx, 30*time.Second); err != nil {
		sourceDB.Stop(ctx)
		targetDB.Stop(ctx)
		t.Fatalf("target database not ready: %v", err)
	}

	return &TestEnvironment{
		SourceDB: sourceDB,
		TargetDB: targetDB,
	}
}

// Cleanup stops all containers in the test environment
func (te *TestEnvironment) Cleanup(ctx context.Context) {
	if te.SourceDB != nil {
		te.SourceDB.Stop(ctx)
	}
	if te.TargetDB != nil {
		te.TargetDB.Stop(ctx)
	}
}

// GetSourceConnectionString returns the source database connection string
func (te *TestEnvironment) GetSourceConnectionString() string {
	return te.SourceDB.GetConnectionString()
}

// GetTargetConnectionString returns the target database connection string
func (te *TestEnvironment) GetTargetConnectionString() string {
	return te.TargetDB.GetConnectionString()
}
