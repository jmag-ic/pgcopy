package schema

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the YAML configuration structure
type Config struct {
	Schemas []Schema `yaml:"schemas"`
}

// Schema represents a database schema
type Schema struct {
	Name   string  `yaml:"name"`
	Tables []Table `yaml:"tables"`
}

// Table represents a database table
type Table struct {
	Name      string            `yaml:"name"`
	Ignore    []string          `yaml:"ignore,omitempty"`
	Transform map[string]string `yaml:"transform,omitempty"`
	Filter    string            `yaml:"filter,omitempty"`
	Truncate  bool              `yaml:"truncate,omitempty"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML config: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if len(config.Schemas) == 0 {
		return fmt.Errorf("no schemas defined")
	}

	for i, schema := range config.Schemas {
		if schema.Name == "" {
			return fmt.Errorf("schema %d has no name", i)
		}

		if len(schema.Tables) == 0 {
			return fmt.Errorf("schema '%s' has no tables", schema.Name)
		}

		for j, table := range schema.Tables {
			if table.Name == "" {
				return fmt.Errorf("table %d in schema '%s' has no name", j, schema.Name)
			}

			// Validate that a column is not both ignored and transformed
			for _, ignoredCol := range table.Ignore {
				if _, exists := table.Transform[ignoredCol]; exists {
					return fmt.Errorf("table '%s' in schema '%s': column '%s' cannot be both ignored and transformed",
						table.Name, schema.Name, ignoredCol)
				}
			}
		}
	}

	return nil
}

// GetAllTables returns all tables from all schemas
func (c *Config) GetAllTables() []TableInfo {
	var tables []TableInfo
	for _, schema := range c.Schemas {
		for _, table := range schema.Tables {
			tables = append(tables, TableInfo{
				Schema:    schema.Name,
				Table:     table.Name,
				Ignore:    table.Ignore,
				Transform: table.Transform,
				Filter:    table.Filter,
				Truncate:  table.Truncate,
			})
		}
	}
	return tables
}

// TableInfo represents table information for copying
type TableInfo struct {
	Schema    string
	Table     string
	Ignore    []string
	Transform map[string]string
	Filter    string
	Truncate  bool
}
