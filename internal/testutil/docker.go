package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PostgresContainer represents a PostgreSQL container
type PostgresContainer struct {
	Container testcontainers.Container
	Host      string
	Port      string
	User      string
	Password  string
	Database  string
}

// PostgresConfig holds configuration for PostgreSQL container
type PostgresConfig struct {
	User     string
	Password string
	Database string
	Port     string
}

// DefaultPostgresConfig returns a default PostgreSQL configuration
func DefaultPostgresConfig() PostgresConfig {
	return PostgresConfig{
		User:     "testuser",
		Password: "testpass",
		Database: "testdb",
		Port:     "5432",
	}
}

// StartPostgresContainer starts a PostgreSQL container
func StartPostgresContainer(ctx context.Context, config PostgresConfig) (*PostgresContainer, error) {
	// Create container request
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{config.Port + "/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     config.User,
			"POSTGRES_PASSWORD": config.Password,
			"POSTGRES_DB":       config.Database,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	// Start container
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Get container info
	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(config.Port+"/tcp"))
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	return &PostgresContainer{
		Container: container,
		Host:      host,
		Port:      mappedPort.Port(),
		User:      config.User,
		Password:  config.Password,
		Database:  config.Database,
	}, nil
}

// Stop stops and removes the PostgreSQL container
func (pc *PostgresContainer) Stop(ctx context.Context) error {
	return pc.Container.Terminate(ctx)
}

// GetConnectionString returns the PostgreSQL connection string
func (pc *PostgresContainer) GetConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pc.User, pc.Password, pc.Host, pc.Port, pc.Database)
}

// WaitForReady waits for the PostgreSQL container to be ready
func (pc *PostgresContainer) WaitForReady(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Try to connect to PostgreSQL
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to be ready")
		default:
			// Test actual PostgreSQL connection
			pool, err := pgxpool.New(ctx, pc.GetConnectionString())
			if err == nil {
				// Test a simple query
				var result int
				err = pool.QueryRow(ctx, "SELECT 1").Scan(&result)
				pool.Close()
				if err == nil && result == 1 {
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
