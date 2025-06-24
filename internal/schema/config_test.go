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
