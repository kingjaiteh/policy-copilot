# Policy Copilot

A retrieval + classification pipeline over federal privacy and security policy
documents: Privacy Act **SORNs** (System of Records Notices) and **NIST SP
800-53 Rev 5** controls. Documents land in a Unity Catalog bronze Delta table,
get modeled into chunks with dbt, and feed a vector index the copilot queries.

## Layout

```
.
├── app/        FastAPI ingestion API (Week 1). /ingest MERGEs into bronze.
│   ├── app.py
│   ├── app.yaml           Databricks Apps manifest; expects a `sql_warehouse` resource
│   └── requirements.txt   app runtime deps only
├── sql/        Warehouse-side SQL, run in a Databricks SQL editor or notebook
│   ├── 01_setup_bronze.sql      catalog/schema/table DDL
│   └── 02_explore_metadata.sql  reading the JSON `metadata` string column
├── scripts/    Local Python. Run from this directory.
│   ├── sorn_parser.py           .docx -> structured SORN record
│   ├── post_sample_docs.py      parse local SORNs, POST to the app
│   ├── load_to_delta.py         parse local SORNs, write to bronze via the SDK
│   ├── fetch_nist_controls.py   NIST OSCAL catalog -> ingest payloads
│   └── verify_docx_parser.py    parser smoke test on a synthetic .docx
├── dbt/        bronze -> silver -> gold transforms (Week 2). See dbt/README.md.
│   ├── models/silver/   stg_sorn, stg_nist_control, sorn_sections, sorn_routine_uses
│   ├── models/gold/     document_chunks  <- the table the vector index reads
│   └── tests/           singular tests
└── data/       Local corpora and query exports. Gitignored, not part of the repo.
    └── sorn_docx/       the 26 SORN .docx source files
```

`sorn_parser.py` sits in `scripts/` rather than a package directory because the
three scripts that use it do a plain `import sorn_parser`, which resolves
against the script's own directory. Keep them together or the imports break.

## Bronze layer (Week 1)

1. **Create the catalog/schema/table.** Run `sql/01_setup_bronze.sql` in a SQL
   editor or notebook in your workspace.

2. **Load documents.** Two paths write the same MERGE into the same table:

   - **SDK, from your terminal** (`scripts/load_to_delta.py`) — no app to
     deploy or keep alive. This is the path in use.
     ```powershell
     $env:DATABRICKS_HOST = "https://dbc-1d037021-8869.cloud.databricks.com"
     $env:DATABRICKS_WAREHOUSE_ID = "d28a870cbb9ba3dd"
     databricks auth login --host $env:DATABRICKS_HOST
     python scripts/load_to_delta.py --dry-run
     ```
   - **HTTP, via the Databricks App** (`app/`, then
     `scripts/post_sample_docs.py`) — for ingesting from somewhere that has no
     Databricks credentials. Deploying it requires uploading `app/`, attaching
     a SQL warehouse resource keyed `sql_warehouse`, and granting the app's
     service principal `USE CATALOG` / `USE SCHEMA` / `MODIFY` on
     `policy_copilot.bronze.raw_documents`.

   Both MERGE on `(doc_type, source_id)`, so re-running updates rather than
   duplicates.

3. **Verify.**
   ```sql
   SELECT doc_type, source_id, ingested_at
   FROM policy_copilot.bronze.raw_documents
   ORDER BY ingested_at DESC;
   ```

## Transforms (Week 2)

See **[dbt/README.md](dbt/README.md)** for lineage, setup, and the test tiering.

```powershell
cd dbt
dbt deps
dbt build
```

Output is `policy_copilot.gold.document_chunks`.

## Dependencies

Python 3.13, system install (no virtualenv is committed).

| What | Needs |
|------|-------|
| `app/` | `fastapi`, `uvicorn`, `databricks-sdk`, `pydantic` (see `app/requirements.txt`) |
| `scripts/load_to_delta.py` | `databricks-sdk` |
| `scripts/post_sample_docs.py`, `fetch_nist_controls.py` | `requests` |
| `dbt/` | `dbt-databricks` |
