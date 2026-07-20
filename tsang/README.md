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
      "id_column": "event_id",
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
            "id_column": "event_id",
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

Parsing (`parse` command):

- SELECT with columns, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, OFFSET
- INSERT with single/multiple rows
- UPDATE with WHERE
- DELETE with WHERE
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Arithmetic and logical expressions

Pipeline generation (`generate` command) supports a subset: plain SELECT with a WHERE clause of AND-combined comparisons and IN lists. Queries using JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, or OR conditions are rejected with an error rather than silently ignored.

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
