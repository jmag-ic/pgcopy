package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartPostgresContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	config := DefaultPostgresConfig()

	// Start container
	container, err := StartPostgresContainer(ctx, config)
	require.NoError(t, err)
	defer container.Stop(ctx)

	// Wait for container to be ready
	err = container.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	// Verify container properties
	assert.NotEmpty(t, container.Host)
	assert.NotEmpty(t, container.Port)
	assert.Equal(t, config.User, container.User)
	assert.Equal(t, config.Password, container.Password)
	assert.Equal(t, config.Database, container.Database)

	// Verify connection string
	connStr := container.GetConnectionString()
	assert.Contains(t, connStr, container.Host)
	assert.Contains(t, connStr, container.Port)
	assert.Contains(t, connStr, container.User)
	assert.Contains(t, connStr, container.Password)
	assert.Contains(t, connStr, container.Database)
}

func TestStartMultiplePostgresContainers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start first container
	config1 := PostgresConfig{
		User:     "user1",
		Password: "pass1",
		Database: "db1",
		Port:     "5432",
	}
	container1, err := StartPostgresContainer(ctx, config1)
	require.NoError(t, err)
	defer container1.Stop(ctx)

	// Start second container
	config2 := PostgresConfig{
		User:     "user2",
		Password: "pass2",
		Database: "db2",
		Port:     "5432",
	}
	container2, err := StartPostgresContainer(ctx, config2)
	require.NoError(t, err)
	defer container2.Stop(ctx)

	// Verify containers are different
	assert.NotEqual(t, container1.Port, container2.Port)
	assert.NotEqual(t, container1.GetConnectionString(), container2.GetConnectionString())

	// Wait for both containers to be ready
	err = container1.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	err = container2.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)
}
