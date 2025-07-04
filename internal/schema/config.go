package schema

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// DatabaseConfig represents database connection configuration
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port,omitempty"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"ssl_mode,omitempty"`
}

// Config represents the YAML configuration structure
type Config struct {
	Source  DatabaseConfig `yaml:"source,omitempty"`
	Target  DatabaseConfig `yaml:"target,omitempty"`
	Schemas []Schema       `yaml:"schemas"`
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

	// Expand environment variables in passwords
	expandEnvironmentVariables(&config)

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// expandEnvironmentVariables expands environment variables in database passwords
func expandEnvironmentVariables(config *Config) {
	if config.Source.Password != "" {
		config.Source.Password = expandEnvVar(config.Source.Password)
	}
	if config.Target.Password != "" {
		config.Target.Password = expandEnvVar(config.Target.Password)
	}
	fmt.Println(config.Source.Password)
	fmt.Println(config.Target.Password)
}

// expandEnvVar expands environment variables in a string
func expandEnvVar(value string) string {
	if !strings.Contains(value, "${") {
		return value
	}

	// Simple environment variable expansion for ${VAR_NAME} format
	return os.ExpandEnv(value)
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	// Validate database configurations if provided
	if err := validateDatabaseConfig(&config.Source, "source"); err != nil {
		return err
	}
	if err := validateDatabaseConfig(&config.Target, "target"); err != nil {
		return err
	}

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

// validateDatabaseConfig validates a database configuration
func validateDatabaseConfig(db *DatabaseConfig, name string) error {
	// If the database config is empty, it's valid (will use command line args)
	if db.Host == "" && db.Database == "" && db.Username == "" {
		return nil
	}

	if db.Host == "" {
		return fmt.Errorf("%s database: host is required", name)
	}
	if db.Database == "" {
		return fmt.Errorf("%s database: database name is required", name)
	}
	if db.Username == "" {
		return fmt.Errorf("%s database: username is required", name)
	}

	// Set default port if not specified
	if db.Port == 0 {
		db.Port = 5432
	}

	// Set default SSL mode if not specified
	if db.SSLMode == "" {
		db.SSLMode = "prefer"
	}

	return nil
}

// BuildConnectionString builds a PostgreSQL connection string from DatabaseConfig
func (db *DatabaseConfig) BuildConnectionString() string {
	if db.Host == "" {
		return ""
	}

	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s",
		db.Host, db.Port, db.Database, db.Username)

	if db.Password != "" {
		connStr += fmt.Sprintf(" password=%s", db.Password)
	}

	if db.SSLMode != "" {
		connStr += fmt.Sprintf(" sslmode=%s", db.SSLMode)
	}

	return connStr
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
