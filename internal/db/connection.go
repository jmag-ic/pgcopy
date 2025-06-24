package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// Connection represents a database connection
type Connection struct {
	pool *pgxpool.Pool
	url  string
}

// NewConnection creates a new database connection
func NewConnection(ctx context.Context, url string) (*Connection, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Set reasonable defaults
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Info().Str("url", maskPassword(url)).Msg("Database connection established")

	return &Connection{
		pool: pool,
		url:  url,
	}, nil
}

// Close closes the database connection
func (c *Connection) Close() {
	if c.pool != nil {
		c.pool.Close()
		log.Info().Str("url", maskPassword(c.url)).Msg("Database connection closed")
	}
}

// GetPool returns the underlying connection pool
func (c *Connection) GetPool() *pgxpool.Pool {
	return c.pool
}

// maskPassword masks the password in the connection string for logging
func maskPassword(url string) string {
	// Simple masking - in production you might want more sophisticated masking
	if len(url) > 10 {
		return url[:10] + "***"
	}
	return "***"
}

// TestConnection tests if the connection is still valid
func (c *Connection) TestConnection(ctx context.Context) error {
	return c.pool.Ping(ctx)
}
