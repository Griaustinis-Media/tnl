## Usage

### Parse SQL to AST
```bash
# Parse and pretty print
./bin/tsang-codegen parse --sql "SELECT * FROM users" --pretty

# Parse from file and save to JSON
./bin/tsang-codegen parse --file query.sql > ast.json
```

### Generate Pipeline from SQL
```bash
# From SQL string
./bin/tsang-codegen generate \
  --sql "SELECT event_ts, user_id FROM events.tracking" \
  --config config.json

# From SQL file
./bin/tsang-codegen generate \
  --file queries/tracking_events.sql \
  --name tracking-pipeline \
  --output ./pipelines

# From AST file (legacy)
./bin/tsang-codegen generate \
  --ast ast.json \
  --config config.json
```

### Batch Generation

The batch config now supports three formats:
- `"sql": "SELECT ..."` - Inline SQL
- `"sql_file": "path/to/query.sql"` - SQL from file
- `"ast_file": "path/to/ast.json"` - Pre-generated AST
```bash
./bin/tsang-codegen batch_generate \
  --batch pipelines.json \
  --output ./generated-pipelines
```
