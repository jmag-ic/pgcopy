package schema

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		expectError bool
		expected    *Config
	}{
		{
			name: "valid config",
			yamlContent: `
schemas:
  - name: public
    tables:
      - name: users
        ignore: [password_hash]
      - name: products
        ignore: [internal_notes]
`,
			expectError: false,
			expected: &Config{
				Schemas: []Schema{
					{
						Name: "public",
						Tables: []Table{
							{
								Name:   "users",
								Ignore: []string{"password_hash"},
							},
							{
								Name:   "products",
								Ignore: []string{"internal_notes"},
							},
						},
					},
				},
			},
		},
		{
			name: "empty schemas",
			yamlContent: `
schemas: []
`,
			expectError: true,
		},
		{
			name: "schema without name",
			yamlContent: `
schemas:
  - tables:
      - name: users
`,
			expectError: true,
		},
		{
			name: "schema without tables",
			yamlContent: `
schemas:
  - name: public
    tables: []
`,
			expectError: true,
		},
		{
			name: "table without name",
			yamlContent: `
schemas:
  - name: public
    tables:
      - ignore: [password_hash]
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary file
			tmpFile, err := os.CreateTemp("", "config-*.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			// Write YAML content
			_, err = tmpFile.WriteString(tt.yamlContent)
			require.NoError(t, err)
			tmpFile.Close()

			// Load config
			config, err := LoadConfig(tmpFile.Name())

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, config)
		})
	}
}

func TestConfig_GetAllTables(t *testing.T) {
	config := &Config{
		Schemas: []Schema{
			{
				Name: "public",
				Tables: []Table{
					{
						Name:   "users",
						Ignore: []string{"password_hash"},
					},
					{
						Name:   "products",
						Ignore: nil,
					},
				},
			},
			{
				Name: "analytics",
				Tables: []Table{
					{
						Name:   "page_views",
						Filter: "created_at >= '2024-01-01'",
					},
				},
			},
		},
	}

	tables := config.GetAllTables()

	expected := []TableInfo{
		{
			Schema: "public",
			Table:  "users",
			Ignore: []string{"password_hash"},
			Filter: "",
		},
		{
			Schema: "public",
			Table:  "products",
			Ignore: nil,
			Filter: "",
		},
		{
			Schema: "analytics",
			Table:  "page_views",
			Ignore: nil,
			Filter: "created_at >= '2024-01-01'",
		},
	}

	assert.Equal(t, expected, tables)
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("nonexistent.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read config file")
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	// Create temporary file with invalid YAML
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("invalid: yaml: content: [")
	require.NoError(t, err)
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse YAML config")
}

func TestLoadConfigWithTruncate(t *testing.T) {
	yamlContent := `
schemas:
  - name: public
    tables:
      - name: users
        truncate: true
        ignore: ["password_hash"]
      - name: products
        truncate: false
        filter: "price > 100"
      - name: orders
        truncate: true
  - name: analytics
    tables:
      - name: page_views
        truncate: true
        transform:
          user_id: "hash"
`

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write YAML content
	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	// Load config
	config, err := LoadConfig(tmpFile.Name())
	require.NoError(t, err)
	require.Len(t, config.Schemas, 2)

	// Test public schema
	publicSchema := config.Schemas[0]
	assert.Equal(t, "public", publicSchema.Name)
	require.Len(t, publicSchema.Tables, 3)

	// Test users table
	usersTable := publicSchema.Tables[0]
	assert.Equal(t, "users", usersTable.Name)
	assert.True(t, usersTable.Truncate)
	assert.Equal(t, []string{"password_hash"}, usersTable.Ignore)

	// Test products table
	productsTable := publicSchema.Tables[1]
	assert.Equal(t, "products", productsTable.Name)
	assert.False(t, productsTable.Truncate)
	assert.Equal(t, "price > 100", productsTable.Filter)

	// Test orders table
	ordersTable := publicSchema.Tables[2]
	assert.Equal(t, "orders", ordersTable.Name)
	assert.True(t, ordersTable.Truncate)

	// Test analytics schema
	analyticsSchema := config.Schemas[1]
	assert.Equal(t, "analytics", analyticsSchema.Name)
	require.Len(t, analyticsSchema.Tables, 1)

	pageViewsTable := analyticsSchema.Tables[0]
	assert.Equal(t, "page_views", pageViewsTable.Name)
	assert.True(t, pageViewsTable.Truncate)
	assert.Equal(t, "hash", pageViewsTable.Transform["user_id"])
}

func TestLoadConfigWithDatabaseConfig(t *testing.T) {
	yamlContent := `
source:
  host: "source-db.example.com"
  port: 5432
  database: "source_db"
  username: "source_user"
  password: "source_pass"
  ssl_mode: "require"

target:
  host: "target-db.example.com"
  port: 5433
  database: "target_db"
  username: "target_user"
  password: "target_pass"
  ssl_mode: "disable"

schemas:
  - name: public
    tables:
      - name: users
        ignore: [password_hash]
`

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write YAML content
	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	// Load config
	config, err := LoadConfig(tmpFile.Name())
	require.NoError(t, err)

	// Test source database config
	assert.Equal(t, "source-db.example.com", config.Source.Host)
	assert.Equal(t, 5432, config.Source.Port)
	assert.Equal(t, "source_db", config.Source.Database)
	assert.Equal(t, "source_user", config.Source.Username)
	assert.Equal(t, "source_pass", config.Source.Password)
	assert.Equal(t, "require", config.Source.SSLMode)

	// Test target database config
	assert.Equal(t, "target-db.example.com", config.Target.Host)
	assert.Equal(t, 5433, config.Target.Port)
	assert.Equal(t, "target_db", config.Target.Database)
	assert.Equal(t, "target_user", config.Target.Username)
	assert.Equal(t, "target_pass", config.Target.Password)
	assert.Equal(t, "disable", config.Target.SSLMode)

	// Test schemas are still loaded correctly
	require.Len(t, config.Schemas, 1)
	assert.Equal(t, "public", config.Schemas[0].Name)
	require.Len(t, config.Schemas[0].Tables, 1)
	assert.Equal(t, "users", config.Schemas[0].Tables[0].Name)
}

func TestLoadConfigWithEnvironmentVariables(t *testing.T) {
	// Set environment variables
	os.Setenv("SOURCE_PASSWORD", "env_source_pass")
	os.Setenv("TARGET_PASSWORD", "env_target_pass")
	defer func() {
		os.Unsetenv("SOURCE_PASSWORD")
		os.Unsetenv("TARGET_PASSWORD")
	}()

	yamlContent := `
source:
  host: "source-db.example.com"
  database: "source_db"
  username: "source_user"
  password: "${SOURCE_PASSWORD}"

target:
  host: "target-db.example.com"
  database: "target_db"
  username: "target_user"
  password: "${TARGET_PASSWORD}"

schemas:
  - name: public
    tables:
      - name: users
`

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write YAML content
	_, err = tmpFile.WriteString(yamlContent)
	require.NoError(t, err)
	tmpFile.Close()

	// Load config
	config, err := LoadConfig(tmpFile.Name())
	require.NoError(t, err)

	// Test environment variable expansion
	assert.Equal(t, "env_source_pass", config.Source.Password)
	assert.Equal(t, "env_target_pass", config.Target.Password)

	// Test default values
	assert.Equal(t, 5432, config.Source.Port)
	assert.Equal(t, 5432, config.Target.Port)
	assert.Equal(t, "prefer", config.Source.SSLMode)
	assert.Equal(t, "prefer", config.Target.SSLMode)
}

func TestDatabaseConfig_BuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   DatabaseConfig
		expected string
	}{
		{
			name: "complete config",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "testuser",
				Password: "testpass",
				SSLMode:  "require",
			},
			expected: "host=localhost port=5432 dbname=testdb user=testuser password=testpass sslmode=require",
		},
		{
			name: "config without password",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "testuser",
				SSLMode:  "disable",
			},
			expected: "host=localhost port=5432 dbname=testdb user=testuser sslmode=disable",
		},
		{
			name: "config without ssl mode",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "testuser",
				Password: "testpass",
			},
			expected: "host=localhost port=5432 dbname=testdb user=testuser password=testpass",
		},
		{
			name:     "empty config",
			config:   DatabaseConfig{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.BuildConnectionString()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateDatabaseConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      DatabaseConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: DatabaseConfig{
				Host:     "localhost",
				Database: "testdb",
				Username: "testuser",
			},
			expectError: false,
		},
		{
			name:        "empty config",
			config:      DatabaseConfig{},
			expectError: false,
		},
		{
			name: "missing host",
			config: DatabaseConfig{
				Database: "testdb",
				Username: "testuser",
			},
			expectError: true,
		},
		{
			name: "missing database",
			config: DatabaseConfig{
				Host:     "localhost",
				Username: "testuser",
			},
			expectError: true,
		},
		{
			name: "missing username",
			config: DatabaseConfig{
				Host:     "localhost",
				Database: "testdb",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDatabaseConfig(&tt.config, "test")
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
