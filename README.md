# pgcopy

A CLI tool written in Go to copy PostgreSQL tables from a source database to a target database using streaming via COPY protocol.

## Features

- **High Performance**: Uses PostgreSQL COPY protocol for efficient data streaming
- **Flexible Configuration**: YAML-based configuration with column filtering and data filtering
- **Table Truncation**: Option to truncate target tables before copying for clean data migration
- **Dry Run Mode**: Preview what would be copied without executing
- **Comprehensive Logging**: Structured logging with progress reporting

## Usage

### Basic Usage

```bash
pgcopy \
  --source "postgres://user:pass@source-host:5432/source_db" \
  --target "postgres://user:pass@target-host:5432/target_db" \
  --file config.yaml
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

The configuration file is in YAML format and defines which schemas and tables to copy:

```yaml
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

- **schemas**: List of database schemas to copy
  - **name**: Schema name
  - **tables**: List of tables in the schema
    - **name**: Table name
    - **ignore** (optional): List of columns to exclude from copying
    - **transform** (optional): Map of column names to transformation expressions
    - **filter** (optional): SQL WHERE clause to filter rows
    - **truncate** (optional): Boolean to truncate the table before copying

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
| `--source` | PostgreSQL connection string for source database | Yes | - |
| `--target` | PostgreSQL connection string for target database | Yes | - |
| `--file` | YAML configuration file | Yes | - |
| `--dry-run` | Show what would be copied without executing | No | false |

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

### Copy Production Data to Staging

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