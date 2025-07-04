# pgcopy

A CLI tool written in Go to copy PostgreSQL tables from a source database to a target database using streaming via COPY protocol.

## Features

- **High Performance**: Uses PostgreSQL COPY protocol for efficient data streaming
- **Flexible Configuration**: YAML-based configuration with column filtering and data filtering
- **Database Connections**: Support for database connections via config file or command line flags
- **Environment Variables**: Secure password handling with environment variable expansion
- **Table Truncation**: Option to truncate target tables before copying for clean data migration
- **Dry Run Mode**: Preview what would be copied without executing
- **Comprehensive Logging**: Structured logging with progress reporting

## Usage

### Basic Usage with Command Line Flags

```bash
pgcopy \
  --source "postgres://user:pass@source-host:5432/source_db" \
  --target "postgres://user:pass@target-host:5432/target_db" \
  --file config.yaml
```

### Basic Usage with Config File Database Connections

```bash
pgcopy --file config.yaml
```

### Dry Run Mode

```bash
pgcopy \
  --source "postgres://user:pass@source-host:5432/source_db" \
  --target "postgres://user:pass@target-host:5432/target_db" \
  --file config.yaml \
  --dry-run
```

## Configuration File Format

The configuration file is in YAML format and defines database connections and which schemas and tables to copy:

```yaml
# Database connections (optional - can also use command line flags)
source:
  host: "source-db.example.com"
  port: 5432
  database: "source_db"
  username: "source_user"
  password: "${SOURCE_PASSWORD}"  # Environment variable support
  ssl_mode: "require"

target:
  host: "target-db.example.com"
  port: 5432
  database: "target_db"
  username: "target_user"
  password: "${TARGET_PASSWORD}"  # Environment variable support
  ssl_mode: "require"

# Table configuration
schemas:
  - name: public
    tables:
      - name: users
        ignore: 
          - password_hash
          - internal_notes
        transform:
          email: "hash"
        filter: "active = true"
        truncate: true  # Truncate this table before copying
      
      - name: products
        ignore:
          - cost
        transform:
          price: "ROUND(price * 1.1, 2)"  # Add 10% markup
        truncate: false  # Do not truncate this table
      
      - name: orders
        filter: "status != 'cancelled'"
        transform:
          credit_card: "'****-****-****-' || RIGHT(credit_card, 4)"
        # No truncate specified - table will not be truncated
  
  - name: analytics
    tables:
      - name: page_views
        filter: "created_at >= '2024-01-01'"
        transform:
          user_id: "anonymize"
          ip_address: "'192.168.1.0'"
      
      - name: user_sessions
        ignore:
          - session_data
```

### Configuration Options

#### Database Connections

- **source** (optional): Source database connection configuration
  - **host**: Database hostname or IP address
  - **port** (optional): Database port (default: 5432)
  - **database**: Database name
  - **username**: Database username
  - **password**: Database password (supports environment variables like `${PASSWORD}`)
  - **ssl_mode** (optional): SSL mode (default: "prefer")

- **target** (optional): Target database connection configuration
  - Same structure as source configuration

#### Table Configuration

- **schemas**: List of database schemas to copy
  - **name**: Schema name
  - **tables**: List of tables in the schema
    - **name**: Table name
    - **ignore** (optional): List of columns to exclude from copying
    - **transform** (optional): Map of column names to transformation expressions
    - **filter** (optional): SQL WHERE clause to filter rows
    - **truncate** (optional): Boolean to truncate the table before copying

### Environment Variable Support

Database passwords can be securely stored using environment variables:

```yaml
source:
  host: "source-db.example.com"
  database: "source_db"
  username: "source_user"
  password: "${SOURCE_PASSWORD}"  # Will be replaced with SOURCE_PASSWORD env var

target:
  host: "target-db.example.com"
  database: "target_db"
  username: "target_user"
  password: "${TARGET_PASSWORD}"  # Will be replaced with TARGET_PASSWORD env var
```

Then set the environment variables:
```bash
export SOURCE_PASSWORD="your_source_password"
export TARGET_PASSWORD="your_target_password"
pgcopy --file config.yaml
```

### Built-in Transformations

The following built-in transformation functions are available:

- **hash**: `encode(sha256($1::text::bytea), 'hex')` - Creates SHA256 hash of the column value
- **redact**: `'***REDACTED***'` - Replaces the column value with a redacted string
- **anonymize**: `'anon-' || encode(sha256($1::text::bytea), 'hex')` - Creates an anonymous identifier
- **nullify**: `NULL` - Sets the column value to NULL
- **default**: `COALESCE($1, NULL)` - Uses the original value or NULL if empty

### Custom Transformations

You can also use custom SQL expressions by replacing `$1` with the column name:

```yaml
transform:
  email: "UPPER($1)"
  price: "ROUND($1 * 1.1, 2)"
  status: "CASE WHEN $1 = 'active' THEN 'enabled' ELSE 'disabled' END"
```

## Command Line Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--source` | PostgreSQL connection string for source database | No* | - |
| `--target` | PostgreSQL connection string for target database | No* | - |
| `--file` | YAML configuration file | Yes | - |
| `--dry-run` | Show what would be copied without executing | No | false |

*Either provide database connections in the config file OR use command line flags

### Connection Precedence

When both command line flags and config file database connections are provided, **command line flags take precedence**:

- `--source` flag overrides `source:` section in config file
- `--target` flag overrides `target:` section in config file

This allows you to:
- Use config file for default connections
- Override specific connections via command line when needed
- Mix and match (e.g., source from config, target from command line)

Example:
```bash
# Both connections from config file
pgcopy --file config.yaml

# Source from command line, target from config file
pgcopy --file config.yaml --source "postgres://user:pass@host:5432/db"

# Both connections from command line (overrides config file)
pgcopy --file config.yaml \
  --source "postgres://user:pass@host:5432/db" \
  --target "postgres://user:pass@host:5432/db"
```

## Connection String Format

PostgreSQL connection strings follow the standard format:

```
postgres://username:password@host:port/database?sslmode=require
```

Example:
```
postgres://myuser:mypassword@localhost:5432/mydatabase?sslmode=require
```

## Performance Considerations

- **Streaming**: The tool uses PostgreSQL COPY protocol for efficient data streaming between databases
- **Network**: Ensure good network connectivity between source and target databases
- **Memory**: The tool streams data in chunks to minimize memory usage
- **Indexes**: Consider dropping indexes on target tables before copying and recreating them afterward for better performance

## Examples

### Copy Production Data to Staging with Config File

```yaml
# config.yaml
source:
  host: "prod-db.example.com"
  database: "production"
  username: "prod_user"
  password: "${PROD_PASSWORD}"
  ssl_mode: "require"

target:
  host: "staging-db.example.com"
  database: "staging"
  username: "staging_user"
  password: "${STAGING_PASSWORD}"
  ssl_mode: "require"

schemas:
  - name: public
    tables:
      - name: users
        filter: "active = true"
        ignore: ["password_hash"]
      - name: products
        truncate: true
```

```bash
export PROD_PASSWORD="your_prod_password"
export STAGING_PASSWORD="your_staging_password"
pgcopy --file config.yaml
```

### Copy Production Data to Staging with Command Line Flags

```bash
pgcopy \
  --source "postgres://prod_user:prod_pass@prod-db:5432/production" \
  --target "postgres://staging_user:staging_pass@staging-db:5432/staging" \
  --file production-to-staging.yaml
```

## Development

### Building

```bash
go build -o pgcopy main.go
```

### Testing

```bash
go test ./...
```

### Running Tests with Coverage

```bash
go test -cover ./...
```

## License

MIT License - see LICENSE file for details.