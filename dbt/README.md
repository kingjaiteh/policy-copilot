# Policy Copilot - dbt transforms (Week 2)

Bronze to silver to gold, as declarative SQL with lineage and tests.
Output is `policy_copilot.gold.document_chunks`, the table the AI Search
index reads from.

## Lineage

```
bronze.raw_documents (source, written by the FastAPI /ingest app)
   |
   +-- stg_sorn ------------+-- sorn_sections ------+
   |                        |                       |
   |                        +-- sorn_routine_uses --+--> gold.document_chunks
   |                                                |
   +-- stg_nist_control ----------------------------+
```

## Setup

```powershell
pip install dbt-databricks
```

Copy `profiles.yml.example` to `C:\Users\omarj\.dbt\profiles.yml`. The
connection values are already filled in. It uses `auth_type: oauth`, which
reuses the `databricks auth login` session you already have, so there is no
token to create.

```powershell
dbt debug     # verifies connection
dbt deps      # installs dbt_utils
dbt build     # runs models AND tests, in dependency order
```

`dbt build` is the one to use day to day: it interleaves running and testing,
so a model whose upstream tests failed does not get built on bad data.
`dbt run` skips tests entirely.

## Concepts

- **source**: a table dbt reads but does not create. Declaring
  `bronze.raw_documents` as a source is what puts it in the lineage graph and
  lets you run freshness checks on it.
- **ref()**: how models reference each other. dbt uses these to infer the DAG
  and build in the right order. Never hardcode a table name between models.
- **materialization**: how a model becomes a physical object. `table` rebuilds
  fully each run; `incremental` builds once then applies only new/changed rows.
- **incremental_strategy: merge**: Delta MERGE under the hood, keyed on
  `unique_key`. Critical here, see below.
- **generic vs singular tests**: generic tests are reusable macros applied to a
  column in YAML (`not_null`, `unique`, `relationships`). A singular test is a
  SQL file that must return zero rows. Use singular for cross-model invariants.
- **severity**: `error` fails the run and blocks downstream models; `warn`
  logs and continues. `warn_if` / `error_if` add row-count thresholds so a soft
  signal escalates once it becomes systemic.
- **generate_schema_name**: the macro deciding what schema a model lands in.
  Overridden in `macros/` because dbt's default appends `+schema` to the
  profile's schema (`silver` + `gold` -> `silver_gold`) instead of replacing
  it. Without the override these models build into the wrong schemas and the
  vector index points at a table that does not exist.

## The one thing not to change

`document_chunks` is configured `incremental` + `merge` with
`delta.enableChangeDataFeed = true`. Both matter:

1. **CDF is a prerequisite.** A Delta Sync vector index on a standard endpoint
   requires Change Data Feed on the source table. It is off by default.
2. **Merge vs rebuild changes cost.** Overwriting the table makes the previous
   state irrelevant, so the index re-embeds every row on every run. With merge
   plus CDF, Databricks recomputes embeddings only for rows that actually
   changed. On Free Edition's single search unit that is the difference between
   a fast loop and waiting on a full re-embed each time.

If you switch this model to `materialized='table'` to debug something,
switch it back before creating the index.

## Test tiering

Mirrors a severity-tiered validation framework:

| Tier | dbt severity | Examples here |
|------|--------------|---------------|
| CRITICAL | `error` | null/duplicate `chunk_id`, parse failures, orphaned sections, enhancements missing a parent |
| WARN | `warn` + thresholds | missing titles, SORNs with zero sections, chunks under 50 or over 6,000 chars |

Plus one singular test, `assert_no_documents_lost`, catching documents that
reach bronze but produce no chunks. That failure is otherwise completely
silent: green run, row present, document invisible to the copilot.
