"""
Batch-load parsed SORNs straight into the bronze Delta table via the SDK.

This bypasses the FastAPI app entirely. The app exists to accept documents over
HTTP from somewhere that has no Databricks credentials; when you are running
from your own terminal with the SDK already authenticated, it is just an extra
hop that has to be deployed and kept alive. Same warehouse, same MERGE, same
table -- one less moving part.

Setup (PowerShell):
    $env:DATABRICKS_HOST = "https://dbc-1d037021-8869.cloud.databricks.com"
    $env:DATABRICKS_WAREHOUSE_ID = "d28a870cbb9ba3dd"
    databricks auth login --host $env:DATABRICKS_HOST   # or set DATABRICKS_TOKEN

Usage:
    python load_to_delta.py --dry-run     # show the batch plan, no SQL
    python load_to_delta.py               # load the SORN corpus
    python load_to_delta.py --create-table

    # Load any /ingest-shaped payloads instead of parsing .docx. This is the
    # NIST path: fetch_nist_controls.py --dry-run writes the file, this loads it.
    python fetch_nist_controls.py --dry-run
    python load_to_delta.py --payloads nist_payloads.json

Why batching
------------
One MERGE per document means one round trip per document, and each round trip
pays the full statement-submission and result-polling cost. At 24 documents
that is 24 sequential waits. Folding N documents into a single MERGE with a
UNION ALL source cuts that to ceil(24/N) round trips without giving up
idempotency, because the MERGE key is still the business key.

Batches are sized by *bytes*, not row count. These payloads range from ~20 KB
to ~110 KB, so a fixed batch of 10 could produce anything from a 200 KB to a
1.1 MB request. A byte budget keeps request size predictable regardless of
which documents happen to land together.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

import sorn_parser

TABLE = "policy_copilot.bronze.raw_documents"

# Conservative default. The binding limit in practice is the size of the HTTP
# request body carrying the parameters, and a smaller budget also keeps any
# single failed batch cheap to retry. Raise it if you want fewer round trips.
DEFAULT_BATCH_BYTES = 800_000

# Rows per batch, bounding bound-parameter count: this many rows x len(COLUMNS)
# parameters per row. 40 x 6 = 240, which stays under the Statement Execution
# API's documented ceiling on parameters per statement with room to spare.
# Only ever binding at the 3-figure level means the ceiling never becomes the
# thing you are debugging.
DEFAULT_BATCH_ROWS = 40

# The corpus lives outside the repo (data/ is gitignored). Resolved from this
# file rather than the CWD so the script works from anywhere.
DEFAULT_DOCS_DIR = Path(__file__).resolve().parent.parent / "data" / "sorn_docx"

# Statement Execution API caps inline waiting at 50s; past that it goes async
# and we poll.
WAIT_TIMEOUT = "50s"
POLL_SECONDS = 3
POLL_DEADLINE_SECONDS = 600

TERMINAL_STATES = {
    StatementState.SUCCEEDED,
    StatementState.FAILED,
    StatementState.CANCELED,
    StatementState.CLOSED,
}

COLUMNS = ["ingestion_id", "doc_type", "source_id", "raw_text", "metadata", "ingested_at"]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  ingestion_id  STRING,
  doc_type      STRING,
  source_id     STRING,
  raw_text      STRING,
  metadata      STRING,
  ingested_at   TIMESTAMP
) USING DELTA
"""


# ---------------------------------------------------------------------------
# ROW BUILDING
# ---------------------------------------------------------------------------
def build_row(record: dict, ingested_at: str) -> dict:
    """Flatten a parsed SORN into the six bronze columns.

    Everything structured goes into the metadata JSON string, which keeps the
    bronze DDL unchanged while still landing what silver needs.
    """
    metadata = {
        **record["metadata"],
        "source_file": record["source_file"],
        "content_sha256": record["content_sha256"],
        "char_count": record["char_count"],
        "parser_version": record["parser_version"],
        "parse_status": record["parse_status"],
        "quality_flags": record["quality_flags"],
        "missing_sections": record["missing_sections"],
        "section_count": len(record["sections"]),
        "routine_use_count": len(record["routine_uses"]),
        "sections": record["sections"],
        "routine_uses": record["routine_uses"],
    }
    return {
        "ingestion_id": str(uuid.uuid4()),
        "doc_type": record["doc_type"],
        "source_id": record["source_id"],
        "raw_text": record["raw_text"],
        # Non-ASCII survives fine as a bound parameter; ensure_ascii=False keeps
        # the payload smaller and the stored JSON readable.
        "metadata": json.dumps(metadata, ensure_ascii=False),
        "ingested_at": ingested_at,
    }


def build_payload_row(payload: dict, ingested_at: str) -> dict:
    """Flatten an /ingest-shaped payload into the six bronze columns.

    The SORN path above starts from a parsed .docx and has to assemble metadata
    itself. This one starts from a payload that is already in the shape the
    FastAPI /ingest endpoint accepts -- doc_type, source_id, raw_text, and a
    metadata dict -- so there is nothing to assemble. That is what lets
    fetch_nist_controls.py --dry-run feed this loader: same MERGE, same
    batching, same idempotency, without deploying the app to receive a POST.
    """
    missing = [k for k in ("doc_type", "source_id", "raw_text") if not payload.get(k)]
    if missing:
        raise ValueError(
            f"payload missing required field(s) {', '.join(missing)}: "
            f"{json.dumps(payload)[:200]}"
        )
    return {
        "ingestion_id": str(uuid.uuid4()),
        "doc_type": payload["doc_type"],
        "source_id": payload["source_id"],
        "raw_text": payload["raw_text"],
        "metadata": json.dumps(payload.get("metadata") or {}, ensure_ascii=False),
        "ingested_at": ingested_at,
    }


def load_payloads(path: Path) -> list[dict]:
    """Read a JSON file of /ingest payloads, as written by --dry-run producers."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError(f"{path} must hold a JSON list of payloads, got {type(data).__name__}")
    return data


def row_bytes(row: dict) -> int:
    return sum(len(str(value).encode("utf-8")) for value in row.values())


def dedupe_rows(rows: list[dict]) -> tuple[list[dict], list[str]]:
    """Collapse rows sharing a business key, keeping the last one.

    This is not cosmetic. Delta MERGE aborts with a "multiple source rows
    matched" error if two rows in the USING relation match the same target row,
    so a duplicate (doc_type, source_id) inside one batch would fail the whole
    batch. Deduping globally rather than per batch also stops two batches from
    fighting over the same row.
    """
    seen: dict[tuple[str, str], dict] = {}
    duplicates: list[str] = []
    for row in rows:
        key = (row["doc_type"], row["source_id"])
        if key in seen:
            duplicates.append(row["source_id"])
        seen[key] = row
    return list(seen.values()), duplicates


def batch_rows(rows: list[dict], budget: int, max_rows: int = DEFAULT_BATCH_ROWS) -> list[list[dict]]:
    """Group rows into batches under BOTH a byte budget and a row cap.

    Sizing by bytes rather than row count is deliberate. Payloads in the SORN
    corpus range from ~21 KB (DHS/USCG-016) to ~110 KB (DHS/USCIS/ICE/CBP-001),
    a 5x spread, so a fixed batch of 10 could produce anything from a 210 KB
    request to a 1.1 MB one depending purely on which documents happened to
    sort together. A byte budget makes request size predictable regardless of
    corpus composition, which is what you want when the failure mode you are
    avoiding is an oversized request body.

    The row cap covers the opposite case, which NIST exposed. A byte budget
    alone bounds the size of the request but says nothing about how many rows
    are in it, and every row binds len(COLUMNS) parameters. NIST controls are
    ~1.3 KB each against the SORNs' ~55 KB, so all 510 of them fit inside one
    800 KB budget -- one statement carrying 3,060 bound parameters, well past
    what the Statement Execution API accepts. Bytes and parameter count are
    independent limits and each needs its own bound.

    A single row larger than the budget still gets its own batch rather than
    being dropped -- it may or may not succeed, but silently discarding a
    document because it is big would be worse.
    """
    batches: list[list[dict]] = []
    current: list[dict] = []
    current_bytes = 0

    for row in rows:
        size = row_bytes(row)
        over_bytes = current_bytes + size > budget
        over_rows = len(current) >= max_rows
        if current and (over_bytes or over_rows):
            batches.append(current)
            current, current_bytes = [], 0
        current.append(row)
        current_bytes += size

    if current:
        batches.append(current)
    return batches


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
def build_merge(rows: list[dict]) -> tuple[str, list[StatementParameterListItem]]:
    """Build one MERGE whose source is a UNION ALL of the batch's rows.

    The whole point of batching: `SELECT ... UNION ALL SELECT ...` with no FROM
    builds an N-row source relation out of nothing but bound parameters, which
    is exactly what MERGE needs on the USING side. Folding 13 documents into
    one statement turns 13 sequential round trips -- each paying statement
    submission plus result polling, and possibly a cold-warehouse wait -- into
    one. Idempotency is untouched, because the MERGE key is still the business
    key rather than anything per-request.

    Every value is a bound :parameter. Nothing is interpolated into the SQL.
    Two independent reasons: SORN text is dense with apostrophes and quotation
    marks that would break a string-built query outright, and interpolating
    document text into SQL is an injection risk even when the documents come
    from a trusted source today.

    Only the leading SELECT carries `AS` aliases; UNION ALL takes its column
    names from the first branch, so repeating them would be noise.
    """
    selects = []
    parameters: list[StatementParameterListItem] = []

    for index, row in enumerate(rows):
        fields = []
        for column in COLUMNS:
            name = f"{column}_{index}"
            # Only the first SELECT needs aliases; UNION ALL takes its column
            # names from the leading branch.
            fields.append(f":{name} AS {column}" if index == 0 else f":{name}")
            parameters.append(
                StatementParameterListItem(
                    name=name,
                    value=row[column],
                    type="TIMESTAMP" if column == "ingested_at" else "STRING",
                )
            )
        selects.append("    SELECT " + ", ".join(fields))

    source = "\n    UNION ALL\n".join(selects)

    statement = f"""
MERGE INTO {TABLE} AS target
USING (
{source}
) AS source
ON  target.doc_type  = source.doc_type
AND target.source_id = source.source_id
WHEN MATCHED THEN UPDATE SET
    target.ingestion_id = source.ingestion_id,
    target.raw_text     = source.raw_text,
    target.metadata     = source.metadata,
    target.ingested_at  = source.ingested_at
WHEN NOT MATCHED THEN INSERT (
    {", ".join(COLUMNS)}
) VALUES (
    {", ".join("source." + c for c in COLUMNS)}
)
"""
    return statement, parameters


def run_statement(client, warehouse_id, statement, parameters=None):
    """Execute and poll to a terminal state, raising on anything but success.

    execute_statement does not raise when the SQL fails, and it also returns
    while the statement is still running -- a cold serverless warehouse can sit
    in PENDING for a minute. Both cases look like success without this check.
    """
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        parameters=parameters,
        wait_timeout=WAIT_TIMEOUT,
    )

    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    while response.status is not None and response.status.state not in TERMINAL_STATES:
        if time.monotonic() > deadline:
            raise TimeoutError("Timed out waiting for the warehouse (it may still be starting).")
        time.sleep(POLL_SECONDS)
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status is None or response.status.state != StatementState.SUCCEEDED:
        detail = response.status.error if response.status else None
        raise RuntimeError(f"Statement did not succeed: {detail or response.status}")

    return response


def merge_receipt(response) -> str:
    """Read num_inserted_rows / num_updated_rows off the MERGE result."""
    try:
        data = response.result.data_array if response.result else None
        if data and response.manifest and response.manifest.schema:
            columns = [c.name for c in response.manifest.schema.columns]
            row = dict(zip(columns, data[0]))
            inserted = int(row.get("num_inserted_rows", 0) or 0)
            updated = int(row.get("num_updated_rows", 0) or 0)
            return f"{inserted} inserted, {updated} updated"
    except Exception:
        # The write already succeeded; a parsing problem here is cosmetic.
        pass
    return "ok"


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=str(DEFAULT_DOCS_DIR),
                        help="directory of .docx files (SORN corpus)")
    parser.add_argument("--payloads", type=Path, default=None,
                        help="JSON file of /ingest payloads to load instead of "
                             "parsing .docx (e.g. nist_payloads.json)")
    parser.add_argument("--dry-run", action="store_true", help="show the plan, run no SQL")
    parser.add_argument("--create-table", action="store_true", help="CREATE TABLE IF NOT EXISTS first")
    parser.add_argument("--batch-bytes", type=int, default=DEFAULT_BATCH_BYTES)
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS,
                        help="max documents per batch, bounding bound-parameter count")
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="also load documents whose body text failed to capture",
    )
    args = parser.parse_args()

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if args.payloads:
        if not args.payloads.exists():
            print(f"No such payloads file: {args.payloads}", file=sys.stderr)
            return 1
        payloads = load_payloads(args.payloads)
        if not payloads:
            print(f"{args.payloads} holds no payloads")
            return 1
        source_label = str(args.payloads)
        total_read = len(payloads)
        skipped = []
        try:
            rows = [build_payload_row(p, ingested_at) for p in payloads]
        except ValueError as exc:
            print(f"Bad payload: {exc}", file=sys.stderr)
            return 1
    else:
        records = sorn_parser.parse_directory(args.dir)
        if not records:
            print(f"No .docx files found in {args.dir}")
            return 1
        source_label = str(args.dir)
        total_read = len(records)
        skipped = [r for r in records if r["parse_status"] == "empty" and not args.include_empty]
        loadable = [r for r in records if r not in skipped]
        rows = [build_row(r, ingested_at) for r in loadable]

    rows, duplicates = dedupe_rows(rows)
    batches = batch_rows(rows, args.batch_bytes, args.batch_rows)

    total_bytes = sum(row_bytes(r) for r in rows)
    print(f"Read {total_read} document(s) from {source_label}")
    print(f"  loadable : {len(rows)}")
    print(f"  skipped  : {len(skipped)} (empty capture)")
    if duplicates:
        print(f"  duplicate source_ids collapsed: {', '.join(duplicates)}")
    print(f"  payload  : {total_bytes:,} bytes")
    print(f"  batches  : {len(batches)} (budget {args.batch_bytes:,} bytes, "
          f"max {args.batch_rows} rows)\n")

    for i, batch in enumerate(batches, 1):
        size = sum(row_bytes(r) for r in batch)
        print(f"  batch {i}: {len(batch):>2} docs, {size:>9,} bytes")
    print()

    if args.dry_run:
        statement, parameters = build_merge(batches[0])
        print(f"Sample MERGE for batch 1: {len(statement):,} chars of SQL, "
              f"{len(parameters)} bound parameters")
        print("Dry run - nothing was sent.")
        return 0

    if "DATABRICKS_WAREHOUSE_ID" not in os.environ:
        print("DATABRICKS_WAREHOUSE_ID is not set.", file=sys.stderr)
        return 1
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    client = WorkspaceClient()

    if args.create_table:
        print("Creating table if needed...")
        run_statement(client, warehouse_id, CREATE_TABLE_SQL)
        print("  ok\n")

    started = time.monotonic()
    failures = 0
    for i, batch in enumerate(batches, 1):
        ids = ", ".join(r["source_id"] for r in batch)
        print(f"  batch {i}/{len(batches)} ({len(batch)} docs)... ", end="", flush=True)
        statement, parameters = build_merge(batch)
        try:
            response = run_statement(client, warehouse_id, statement, parameters)
            print(merge_receipt(response))
        except Exception as exc:
            # One bad batch should not abandon the rest of the corpus. MERGE is
            # idempotent, so a failed batch is safe to re-run on its own.
            failures += 1
            print(f"FAILED\n    {exc}\n    docs in batch: {ids}")

    elapsed = time.monotonic() - started
    print(f"\n{len(batches) - failures}/{len(batches)} batches succeeded in {elapsed:.1f}s.")
    if failures:
        print("Re-running is safe: MERGE upserts on (doc_type, source_id).")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
