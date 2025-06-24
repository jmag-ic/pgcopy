package cmd

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"pgcopy/internal/copy"
	"pgcopy/internal/schema"
)

var (
	sourceDB   string
	targetDB   string
	configFile string
	dryRun     bool
)

// NewRootCmd creates the root command
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pgcopy",
		Short: "A high-performance CLI tool to copy PostgreSQL tables using streaming",
		Long: `pgcopy is a high-performance CLI tool written in Go to copy PostgreSQL tables 
from a source database to a target database using streaming via COPY protocol.`,
		RunE: runCopy,
	}

	// Flags
	rootCmd.Flags().StringVar(&sourceDB, "source", "", "PostgreSQL connection string for source database")
	rootCmd.Flags().StringVar(&targetDB, "target", "", "PostgreSQL connection string for target database")
	rootCmd.Flags().StringVar(&configFile, "file", "", "YAML configuration file")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be copied without executing")

	// Mark required flags
	rootCmd.MarkFlagRequired("source")
	rootCmd.MarkFlagRequired("target")
	rootCmd.MarkFlagRequired("file")

	// Bind flags to viper
	viper.BindPFlag("source", rootCmd.Flags().Lookup("source"))
	viper.BindPFlag("target", rootCmd.Flags().Lookup("target"))
	viper.BindPFlag("file", rootCmd.Flags().Lookup("file"))
	viper.BindPFlag("dry-run", rootCmd.Flags().Lookup("dry-run"))

	return rootCmd
}

func runCopy(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	log.Info().Msg("Starting pgcopy operation")

	// Load configuration
	config, err := schema.LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Create copy engine
	engine, err := copy.NewEngine(sourceDB, targetDB)
	if err != nil {
		return fmt.Errorf("failed to create copy engine: %w", err)
	}
	defer engine.Close()

	// Execute copy operation
	if dryRun {
		log.Info().Msg("DRY RUN MODE - No actual copying will be performed")
		return engine.DryRun(ctx, config)
	}

	return engine.Copy(ctx, config)
}
