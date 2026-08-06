-- Run this in a Databricks SQL editor or notebook (%sql cell) BEFORE deploying the app.
-- Creates the catalog/schema structure and the bronze landing table.

CREATE CATALOG IF NOT EXISTS policy_copilot;

CREATE SCHEMA IF NOT EXISTS policy_copilot.bronze;
CREATE SCHEMA IF NOT EXISTS policy_copilot.silver;
CREATE SCHEMA IF NOT EXISTS policy_copilot.gold;

-- Bronze: raw, as-ingested documents. Nothing is cleaned or chunked here.
-- doc_type distinguishes the two corpora so the agent can reason across them later.
CREATE TABLE IF NOT EXISTS policy_copilot.bronze.raw_documents (
  ingestion_id  STRING,      -- unique id per POST, useful for debugging/replay
  doc_type      STRING,      -- 'sorn' or 'nist_control'
  source_id     STRING,      -- e.g. 'DHS/ALL-024' or 'AC-2'
  raw_text      STRING,      -- the full document/control text as received
  metadata      STRING,      -- JSON string: title, url, agency, effective_date, etc.
  ingested_at   TIMESTAMP
) USING DELTA;

-- Quick sanity check after you POST some docs:
-- SELECT doc_type, source_id, ingested_at FROM policy_copilot.bronze.raw_documents ORDER BY ingested_at DESC;
