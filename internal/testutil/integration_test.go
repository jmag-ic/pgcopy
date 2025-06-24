package testutil

import (
	"context"
	"pgcopy/internal/copy"
	"pgcopy/internal/schema"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyWithTestData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start source container
	sourceContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer sourceContainer.Stop(ctx)

	// Start target container
	targetContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer targetContainer.Stop(ctx)

	// Wait for containers to be ready
	err = sourceContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	err = targetContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	// Create test schema in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Insert test data in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/data.sql")
	require.NoError(t, err)

	// Verify source data was loaded
	sourceStats, err := GetTestDataStats(ctx, sourceContainer.GetConnectionString())
	require.NoError(t, err)
	assert.Greater(t, sourceStats["public.users"], 0)
	assert.Greater(t, sourceStats["public.products"], 0)
	assert.Greater(t, sourceStats["public.orders"], 0)
	assert.Greater(t, sourceStats["analytics.page_views"], 0)
	assert.Greater(t, sourceStats["public.complex_data"], 0)

	// Create test schema in target
	err = RunSqlScript(ctx, targetContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Create copy configuration
	config := &schema.Config{
		Schemas: []schema.Schema{
			{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "users",
						Transform: map[string]string{
							"password_hash": "hash",
						},
					},
					{
						Name:   "products",
						Ignore: []string{"cost"},
					},
					{
						Name: "orders",
					},
					{
						Name: "complex_data",
					},
				},
			},
			{
				Name: "analytics",
				Tables: []schema.Table{
					{
						Name: "page_views",
					},
				},
			},
		},
	}

	// Create copy engine
	engine, err := copy.NewEngine(
		sourceContainer.GetConnectionString(),
		targetContainer.GetConnectionString(),
	)
	require.NoError(t, err)
	defer engine.Close()

	// Perform the copy
	err = engine.Copy(ctx, config)
	require.NoError(t, err)

	// Verify target data
	targetStats, err := GetTestDataStats(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)

	// Verify all tables were copied
	assert.Equal(t, sourceStats["public.users"], targetStats["public.users"])
	assert.Equal(t, sourceStats["public.products"], targetStats["public.products"])
	assert.Equal(t, sourceStats["public.orders"], targetStats["public.orders"])
	assert.Equal(t, sourceStats["analytics.page_views"], targetStats["analytics.page_views"])
	assert.Equal(t, sourceStats["public.complex_data"], targetStats["public.complex_data"])
}

func TestCopyWithFilters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start containers
	sourceContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer sourceContainer.Stop(ctx)

	targetContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer targetContainer.Stop(ctx)

	// Wait for containers to be ready
	err = sourceContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)
	err = targetContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	// Create test schema in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Load test data
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/data.sql")
	require.NoError(t, err)

	// Create test schema in target
	err = RunSqlScript(ctx, targetContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Create configuration with filters
	config := &schema.Config{
		Schemas: []schema.Schema{
			{
				Name: "public",
				Tables: []schema.Table{
					{
						Name:   "products",
						Filter: "price > 100", // Only expensive products
					},
					{
						Name:   "users",
						Filter: "is_active = true", // Only active users
					},
				},
			},
		},
	}

	// Create copy engine
	engine, err := copy.NewEngine(
		sourceContainer.GetConnectionString(),
		targetContainer.GetConnectionString(),
	)
	require.NoError(t, err)
	defer engine.Close()

	// Perform the copy
	err = engine.Copy(ctx, config)
	require.NoError(t, err)

	// Verify filtered data
	sourcePool, err := pgxpool.New(ctx, sourceContainer.GetConnectionString())
	require.NoError(t, err)
	defer sourcePool.Close()

	targetPool, err := pgxpool.New(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)
	defer targetPool.Close()

	// Check products filter
	var sourceProductCount, targetProductCount int
	err = sourcePool.QueryRow(ctx, "SELECT COUNT(*) FROM public.products WHERE price > 100").Scan(&sourceProductCount)
	require.NoError(t, err)
	err = targetPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.products").Scan(&targetProductCount)
	require.NoError(t, err)
	assert.Equal(t, sourceProductCount, targetProductCount)

	// Check users filter
	var sourceUserCount, targetUserCount int
	err = sourcePool.QueryRow(ctx, "SELECT COUNT(*) FROM public.users WHERE is_active = true").Scan(&sourceUserCount)
	require.NoError(t, err)
	err = targetPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.users").Scan(&targetUserCount)
	require.NoError(t, err)
	assert.Equal(t, sourceUserCount, targetUserCount)
}

func TestCopyWithIgnoredColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start containers
	sourceContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer sourceContainer.Stop(ctx)

	targetContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer targetContainer.Stop(ctx)

	// Wait for containers to be ready
	err = sourceContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)
	err = targetContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	// Create test schema in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Insert test data in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/data.sql")
	require.NoError(t, err)

	// Create test schema in target
	err = RunSqlScript(ctx, targetContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Create configuration with ignored columns
	config := &schema.Config{
		Schemas: []schema.Schema{
			{
				Name: "public",
				Tables: []schema.Table{
					{
						Name:   "users",
						Ignore: []string{"email", "last_login"},
					},
					{
						Name:   "products",
						Ignore: []string{"cost", "stock_quantity"},
					},
				},
			},
		},
	}

	// Create copy engine
	engine, err := copy.NewEngine(
		sourceContainer.GetConnectionString(),
		targetContainer.GetConnectionString(),
	)
	require.NoError(t, err)
	defer engine.Close()

	// Perform the copy
	err = engine.Copy(ctx, config)
	require.NoError(t, err)

	// Verify that ignored columns are not present in target
	targetPool, err := pgxpool.New(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)
	defer targetPool.Close()

	// Check that ignored columns don't exist in target
	var dataExists bool
	err = targetPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM public.users
			WHERE email IS NOT NULL OR last_login IS NOT NULL
		)
	`).Scan(&dataExists)
	require.NoError(t, err)
	assert.False(t, dataExists, "no users should exists with email or last_login in target")

	err = targetPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM public.products
			WHERE cost IS NOT NULL AND stock_quantity IS NOT NULL
		)
	`).Scan(&dataExists)
	require.NoError(t, err)
	assert.False(t, dataExists, "no products should exists with cost or stock_quantity in target")
}

func TestCopyComplexDataTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start containers
	sourceContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer sourceContainer.Stop(ctx)

	targetContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer targetContainer.Stop(ctx)

	// Wait for containers to be ready
	err = sourceContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)
	err = targetContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	// Create test schema in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Insert test data in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/data.sql")
	require.NoError(t, err)

	// Create test schema in target
	err = RunSqlScript(ctx, targetContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Create configuration for complex data types
	config := &schema.Config{
		Schemas: []schema.Schema{
			{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "complex_data",
					},
				},
			},
		},
	}

	// Create copy engine
	engine, err := copy.NewEngine(
		sourceContainer.GetConnectionString(),
		targetContainer.GetConnectionString(),
	)
	require.NoError(t, err)
	defer engine.Close()

	// Perform the copy
	err = engine.Copy(ctx, config)
	require.NoError(t, err)

	// Verify complex data types were copied correctly
	sourcePool, err := pgxpool.New(ctx, sourceContainer.GetConnectionString())
	require.NoError(t, err)
	defer sourcePool.Close()

	targetPool, err := pgxpool.New(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)
	defer targetPool.Close()

	// Compare JSONB data
	var sourceJSON, targetJSON string
	err = sourcePool.QueryRow(ctx, "SELECT simple_json::text FROM public.complex_data WHERE name = 'Test Data 1'").Scan(&sourceJSON)
	require.NoError(t, err)
	err = targetPool.QueryRow(ctx, "SELECT simple_json::text FROM public.complex_data WHERE name = 'Test Data 1'").Scan(&targetJSON)
	require.NoError(t, err)
	assert.Equal(t, sourceJSON, targetJSON)

	// Compare array data
	var sourceArray, targetArray []string
	err = sourcePool.QueryRow(ctx, "SELECT string_array FROM public.complex_data WHERE name = 'Test Data 1'").Scan(&sourceArray)
	require.NoError(t, err)
	err = targetPool.QueryRow(ctx, "SELECT string_array FROM public.complex_data WHERE name = 'Test Data 1'").Scan(&targetArray)
	require.NoError(t, err)
	assert.Equal(t, sourceArray, targetArray)
}

func TestCopyWithTruncate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start containers
	sourceContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer sourceContainer.Stop(ctx)

	targetContainer, err := StartPostgresContainer(ctx, DefaultPostgresConfig())
	require.NoError(t, err)
	defer targetContainer.Stop(ctx)

	// Wait for containers to be ready
	err = sourceContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)
	err = targetContainer.WaitForReady(ctx, 30*time.Second)
	require.NoError(t, err)

	// Create test schema in both source and target
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)
	err = RunSqlScript(ctx, targetContainer.GetConnectionString(), "schema/schema.sql")
	require.NoError(t, err)

	// Load test data in source
	err = RunSqlScript(ctx, sourceContainer.GetConnectionString(), "schema/data.sql")
	require.NoError(t, err)

	// Load some initial data in target (to test that truncate removes it)
	// Use different data to avoid primary key conflicts
	err = RunSqlScript(ctx, targetContainer.GetConnectionString(), "schema/data.sql")
	require.NoError(t, err)

	// Verify target has initial data
	targetStatsBefore, err := GetTestDataStats(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)
	assert.Greater(t, targetStatsBefore["public.users"], 0)
	assert.Greater(t, targetStatsBefore["public.products"], 0)

	// Create configuration with truncate enabled for some tables
	// Note: We can't truncate users table due to foreign key constraints from orders
	// So we'll test truncate on tables without foreign key dependencies
	config := &schema.Config{
		Schemas: []schema.Schema{
			{
				Name: "public",
				Tables: []schema.Table{
					{
						Name:     "products",
						Truncate: true, // This should truncate before copy
					},
					{
						Name:     "orders",
						Truncate: true, // This should truncate before copy
					},
					{
						Name:     "complex_data",
						Truncate: false, // This should not truncate
						Transform: map[string]string{
							"id": "id + 1000",
						},
					},
				},
			},
			{
				Name: "analytics",
				Tables: []schema.Table{
					{
						Name:     "page_views",
						Truncate: true, // This should truncate before copy
					},
				},
			},
		},
	}

	// Create copy engine
	engine, err := copy.NewEngine(
		sourceContainer.GetConnectionString(),
		targetContainer.GetConnectionString(),
	)
	require.NoError(t, err)
	defer engine.Close()

	// Perform the copy
	err = engine.Copy(ctx, config)
	require.NoError(t, err)

	// Verify results
	sourceStats, err := GetTestDataStats(ctx, sourceContainer.GetConnectionString())
	require.NoError(t, err)
	targetStatsAfter, err := GetTestDataStats(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)

	// Tables with truncate=true should have exact same count as source
	assert.Equal(t, sourceStats["public.products"], targetStatsAfter["public.products"])
	assert.Equal(t, sourceStats["public.orders"], targetStatsAfter["public.orders"])
	assert.Equal(t, sourceStats["analytics.page_views"], targetStatsAfter["analytics.page_views"])

	// Tables with truncate=false should have original count + source count (duplicated)
	// However, complex_data will fail due to primary key conflicts, so it should remain unchanged
	expectedComplexDataCount := targetStatsBefore["public.complex_data"] + sourceStats["public.complex_data"]
	assert.Equal(t, expectedComplexDataCount, targetStatsAfter["public.complex_data"])

	// Verify that truncate actually worked by checking specific data
	sourcePool, err := pgxpool.New(ctx, sourceContainer.GetConnectionString())
	require.NoError(t, err)
	defer sourcePool.Close()

	targetPool, err := pgxpool.New(ctx, targetContainer.GetConnectionString())
	require.NoError(t, err)
	defer targetPool.Close()

	// Check that products table was truncated (should only have source data, not duplicated)
	var sourceProductCount, targetProductCount int
	err = sourcePool.QueryRow(ctx, "SELECT COUNT(*) FROM public.products").Scan(&sourceProductCount)
	require.NoError(t, err)
	err = targetPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.products").Scan(&targetProductCount)
	require.NoError(t, err)
	assert.Equal(t, sourceProductCount, targetProductCount)

	// Check that complex_data table was NOT truncated (should have both original and new data)
	var targetComplexDataCount int
	err = targetPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.complex_data").Scan(&targetComplexDataCount)
	require.NoError(t, err)
	assert.Equal(t, expectedComplexDataCount, targetComplexDataCount)
}
