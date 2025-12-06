# Tsang

A SQL parser and data pipeline code generator for Clojure.

Tsang parses SQL queries into an Abstract Syntax Tree (AST) and generates complete Clojure data pipeline projects using the Leng universal database library.

## Features

- **SQL Parser**: Tokenizes and parses SQL into structured AST
- **AST to JSON**: Export parsed SQL as JSON for inspection or processing
- **Pipeline Generator**: Generate complete Clojure pipeline projects from SQL queries
- **Batch Generation**: Generate multiple pipelines from a single configuration file
- **Incremental Loading**: Built-in watermark support for incremental data pipelines
- **Multi-Database Support**: Cassandra, PostgreSQL, MongoDB sources → Druid, PostgreSQL sinks

## Installation
```bash
cd tsang

# Install dependencies
gem install liquid

# Make executable
chmod +x bin/tsang
```

## Usage

### Parse SQL to AST

Convert SQL queries to JSON AST for inspection or further processing:
```bash
# Parse and pretty print
./bin/tsang parse --sql "SELECT * FROM users WHERE age > 18" --pretty

# Parse from file
./bin/tsang parse --file queries/my_query.sql > ast.json

# Compact JSON output
./bin/tsang parse --sql "SELECT id, name FROM products"
```

### Generate Pipeline from SQL

Generate a complete Clojure data pipeline project directly from SQL:
```bash
# Generate from SQL string
./bin/tsang generate \
  --sql "SELECT event_ts, user_id, event_type FROM events.tracking_events" \
  --name tracking-pipeline \
  --config config.json

# Generate from SQL file
./bin/tsang generate \
  --file queries/user_sessions.sql \
  --name sessions-pipeline

# Generate with custom output directory
./bin/tsang generate \
  --sql "SELECT * FROM orders" \
  --name orders-pipeline \
  --output ./custom/path
```

**Default output**: `./build/<project-name>/`

### Batch Generate Multiple Pipelines

Generate multiple pipelines from a single batch configuration:
```bash
./bin/tsang batch_generate \
  --batch pipelines.json \
  --output ./build/pipelines
```

**Default output**: `./build/pipelines/<pipeline-name>/`

## Configuration

### Single Pipeline Config (`config.json`)
```json
{
  "project_name": "tracking-events-pipeline",
  "batch_size": 5000,
  "watermark_enabled": true,
  "incremental": true,
  "timestamp_column": "event_ts",
  "leng_path": "../../leng",
  "source_type": "cassandra",
  "sink": {
    "type": "druid",
    "table": "tracking_events",
    "default_url": "https://druid.example.com"
  }
}
```

### Batch Config (`pipelines.json`)
```json
{
  "pipelines": [
    {
      "name": "tracking-events-pipeline",
      "sql": "SELECT event_ts, user_id, event_type FROM events.tracking_events WHERE event_type = 'page_view'",
      "config": {
        "batch_size": 5000,
        "watermark_enabled": true,
        "timestamp_column": "event_ts",
        "source_type": "cassandra",
        "sink": {
          "type": "druid",
          "table": "tracking_events",
          "default_url": "https://druid.example.com"
        }
      }
    },
    {
      "name": "user-sessions-pipeline",
      "sql_file": "queries/user_sessions.sql",
      "config": {
        "batch_size": 10000,
        "watermark_enabled": true,
        "timestamp_column": "session_start",
        "source_type": "cassandra",
        "sink": {
          "type": "druid",
          "table": "user_sessions",
          "default_url": "https://druid.example.com"
        }
      }
    }
  ]
}
```

## Generated Pipeline Structure

Each generated pipeline includes:
```
build/my-pipeline/
├── deps.edn              # Clojure dependencies
├── src/
│   └── pipeline.clj      # Main pipeline code
├── resources/
│   └── logback.xml       # Logging configuration
└── README.md             # Pipeline-specific documentation
```

## Running Generated Pipelines
```bash
cd build/my-pipeline

# Set environment variables
export CASSANDRA_HOST=your-host
export CASSANDRA_USER=your-user
export CASSANDRA_PASSWORD=your-password
export DRUID_URL=https://druid.example.com
export DRUID_USER=your-user
export DRUID_PASSWORD=your-password

# Run incremental load (with watermark)
clj -M:run incremental

# Run full reload (resets watermark)
clj -M:run full-reload

# Run one-time load
clj -M:run
```

## Supported SQL Features

### SELECT Statements
- Column selection: `SELECT id, name, email`
- Wildcards: `SELECT *`
- Table aliases: `FROM users AS u`
- Schema qualification: `FROM events.tracking_events`
- WHERE clauses with conditions
- JOINŠNope, it's not properly formatted. Whole display of document is broken on Claude websiteYou're right, the formatting is breaking the display. Let me give you a script to create the file:
bashcat > README.md << 'ENDOFFILE'
# Tsang

A SQL parser and data pipeline code generator for Clojure.

Tsang parses SQL queries into an Abstract Syntax Tree (AST) and generates complete Clojure data pipeline projects using the Leng universal database library.

## Features

- **SQL Parser**: Tokenizes and parses SQL into structured AST
- **AST to JSON**: Export parsed SQL as JSON for inspection or processing
- **Pipeline Generator**: Generate complete Clojure pipeline projects from SQL queries
- **Batch Generation**: Generate multiple pipelines from a single configuration file
- **Incremental Loading**: Built-in watermark support for incremental data pipelines
- **Multi-Database Support**: Cassandra, PostgreSQL, MongoDB sources → Druid, PostgreSQL sinks

## Installation

Install dependencies and make the binary executable:

    cd tsang
    gem install liquid
    chmod +x bin/tsang

## Usage

### Parse SQL to AST

Convert SQL queries to JSON AST:

    ./bin/tsang parse --sql "SELECT * FROM users WHERE age > 18" --pretty
    ./bin/tsang parse --file queries/my_query.sql > ast.json

### Generate Pipeline from SQL

Generate a complete Clojure data pipeline project:

    ./bin/tsang generate --sql "SELECT event_ts, user_id FROM events.tracking_events" --name tracking-pipeline --config config.json
    ./bin/tsang generate --file queries/user_sessions.sql --name sessions-pipeline

Default output: `./build/<project-name>/`

### Batch Generate Multiple Pipelines

    ./bin/tsang batch_generate --batch pipelines.json --output ./build/pipelines

## Configuration

### Single Pipeline Config (config.json)

    {
      "project_name": "tracking-events-pipeline",
      "batch_size": 5000,
      "watermark_enabled": true,
      "incremental": true,
      "timestamp_column": "event_ts",
      "leng_path": "../../leng",
      "source_type": "cassandra",
      "sink": {
        "type": "druid",
        "table": "tracking_events",
        "default_url": "https://druid.example.com"
      }
    }

### Batch Config (pipelines.json)

    {
      "pipelines": [
        {
          "name": "tracking-events-pipeline",
          "sql": "SELECT event_ts, user_id FROM events.tracking_events",
          "config": {
            "batch_size": 5000,
            "watermark_enabled": true,
            "timestamp_column": "event_ts",
            "source_type": "cassandra",
            "sink": {
              "type": "druid",
              "table": "tracking_events"
            }
          }
        }
      ]
    }

## Generated Pipeline Structure

    build/my-pipeline/
    ├── deps.edn
    ├── src/
    │   └── pipeline.clj
    ├── resources/
    │   └── logback.xml
    └── README.md

## Running Generated Pipelines

    cd build/my-pipeline
    
    export CASSANDRA_HOST=your-host
    export CASSANDRA_USER=your-user
    export CASSANDRA_PASSWORD=your-password
    export DRUID_URL=https://druid.example.com
    export DRUID_USER=your-user
    export DRUID_PASSWORD=your-password
    
    clj -M:run incremental
    clj -M:run full-reload
    clj -M:run

## Supported SQL Features

- SELECT with columns, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, OFFSET
- INSERT with single/multiple rows
- UPDATE with WHERE
- DELETE with WHERE
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Arithmetic and logical expressions

## Supported Databases

**Sources:** Cassandra, PostgreSQL, MongoDB

**Sinks:** Druid, PostgreSQL, Elasticsearch

## CLI Reference

Commands:
- `parse` - Parse SQL to AST JSON
- `generate` - Generate single pipeline
- `batch_generate` - Generate multiple pipelines

Options:
- `-s, --sql SQL` - SQL query string
- `-f, --file FILE` - SQL file path
- `-o, --output DIR` - Output directory (default: ./build)
- `-n, --name NAME` - Project name
- `--config FILE` - Configuration JSON file
- `--batch FILE` - Batch configuration JSON file
- `--pretty` - Pretty print JSON output
- `-h, --help` - Show help message

## Examples

Simple query:

    ./bin/tsang generate --sql "SELECT * FROM users" --name users-pipeline

Complex query with config:

    ./bin/tsang generate \
      --sql "SELECT event_ts, user_id FROM events.tracking_events WHERE event_type = 'page_view'" \
      --name tracking-pipeline \
      --config pipeline_config.json

Batch generation:

    ./bin/tsang batch_generate --batch batch.json

## Development

Run tests:

    bundle install
    bundle exec rspec

Project structure:

    tsang/
    ├── lib/
    │   ├── tsang.rb
    │   ├── lexer.rb
    │   ├── parser.rb
    │   ├── cli.rb
    │   ├── ast/
    │   └── codegen/
    ├── templates/
    ├── spec/
    └── bin/

## Integration with Leng

Generated pipelines use the Leng universal database library located at `../../leng` relative to the generated pipeline.

## License

MIT License

The Tsang parser and code generator are licensed under the MIT License.

Note: Generated pipelines use the Leng library which is licensed under the Eclipse Public License 2.0. Generated code inherits the EPL 2.0 license.
