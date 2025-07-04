package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pgcopy/internal/schema"
)

func TestGetConnectionStrings(t *testing.T) {
	tests := []struct {
		name           string
		config         *schema.Config
		sourceDBFlag   string
		targetDBFlag   string
		expectedSource string
		expectedTarget string
		expectError    bool
	}{
		{
			name: "both from config file",
			config: &schema.Config{
				Source: schema.DatabaseConfig{
					Host:     "config-source.example.com",
					Port:     5432,
					Database: "config_source_db",
					Username: "config_user",
					Password: "config_password",
					SSLMode:  "prefer",
				},
				Target: schema.DatabaseConfig{
					Host:     "config-target.example.com",
					Port:     5432,
					Database: "config_target_db",
					Username: "config_user",
					Password: "config_password",
					SSLMode:  "prefer",
				},
			},
			expectedSource: "host=config-source.example.com port=5432 dbname=config_source_db user=config_user password=config_password sslmode=prefer",
			expectedTarget: "host=config-target.example.com port=5432 dbname=config_target_db user=config_user password=config_password sslmode=prefer",
		},
		{
			name:           "both from command line flags",
			config:         &schema.Config{},
			sourceDBFlag:   "host=cli-source.example.com dbname=cli_source_db user=cli_user",
			targetDBFlag:   "host=cli-target.example.com dbname=cli_target_db user=cli_user",
			expectedSource: "host=cli-source.example.com dbname=cli_source_db user=cli_user",
			expectedTarget: "host=cli-target.example.com dbname=cli_target_db user=cli_user",
		},
		{
			name: "source from command line, target from config",
			config: &schema.Config{
				Target: schema.DatabaseConfig{
					Host:     "config-target.example.com",
					Port:     5432,
					Database: "config_target_db",
					Username: "config_user",
					Password: "config_password",
					SSLMode:  "prefer",
				},
			},
			sourceDBFlag:   "host=cli-source.example.com dbname=cli_source_db user=cli_user",
			expectedSource: "host=cli-source.example.com dbname=cli_source_db user=cli_user",
			expectedTarget: "host=config-target.example.com port=5432 dbname=config_target_db user=config_user password=config_password sslmode=prefer",
		},
		{
			name: "source from config, target from command line",
			config: &schema.Config{
				Source: schema.DatabaseConfig{
					Host:     "config-source.example.com",
					Port:     5432,
					Database: "config_source_db",
					Username: "config_user",
					Password: "config_password",
					SSLMode:  "prefer",
				},
			},
			targetDBFlag:   "host=cli-target.example.com dbname=cli_target_db user=cli_user",
			expectedSource: "host=config-source.example.com port=5432 dbname=config_source_db user=config_user password=config_password sslmode=prefer",
			expectedTarget: "host=cli-target.example.com dbname=cli_target_db user=cli_user",
		},
		{
			name: "command line flags override config file",
			config: &schema.Config{
				Source: schema.DatabaseConfig{
					Host:     "config-source.example.com",
					Port:     5432,
					Database: "config_source_db",
					Username: "config_user",
					Password: "config_password",
					SSLMode:  "prefer",
				},
				Target: schema.DatabaseConfig{
					Host:     "config-target.example.com",
					Port:     5432,
					Database: "config_target_db",
					Username: "config_user",
					Password: "config_password",
					SSLMode:  "prefer",
				},
			},
			sourceDBFlag:   "host=cli-source.example.com dbname=cli_source_db user=cli_user",
			targetDBFlag:   "host=cli-target.example.com dbname=cli_target_db user=cli_user",
			expectedSource: "host=cli-source.example.com dbname=cli_source_db user=cli_user",
			expectedTarget: "host=cli-target.example.com dbname=cli_target_db user=cli_user",
		},
		{
			name:        "no connections provided",
			config:      &schema.Config{},
			expectError: true,
		},
		{
			name: "only source in config, no target",
			config: &schema.Config{
				Source: schema.DatabaseConfig{
					Host:     "config-source.example.com",
					Port:     5432,
					Database: "config_source_db",
					Username: "config_user",
					SSLMode:  "prefer",
				},
			},
			expectError: true,
		},
		{
			name: "only target in config, no source",
			config: &schema.Config{
				Target: schema.DatabaseConfig{
					Host:     "config-target.example.com",
					Port:     5432,
					Database: "config_target_db",
					Username: "config_user",
					SSLMode:  "prefer",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set global variables for the test
			originalSourceDB := sourceDB
			originalTargetDB := targetDB
			defer func() {
				sourceDB = originalSourceDB
				targetDB = originalTargetDB
			}()

			sourceDB = tt.sourceDBFlag
			targetDB = tt.targetDBFlag

			// Call the function
			sourceConnStr, targetConnStr, err := getConnectionStrings(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedSource, sourceConnStr)
			assert.Equal(t, tt.expectedTarget, targetConnStr)
		})
	}
}
