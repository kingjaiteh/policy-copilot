# ---------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------
import os                                    # read environment variables Databricks injects at runtime
import json                                  # metadata arrives as a dict, but Delta column is STRING, so we serialize it
import time                                  # used to poll while a cold warehouse spins up
import uuid                                  # generate a unique id for every POST we receive
from datetime import datetime, timezone      # timestamp each ingestion in UTC

from fastapi import FastAPI, HTTPException   # web framework + a clean way to return error responses
from pydantic import BaseModel, Field        # request body validation (FastAPI uses this automatically)
from databricks.sdk import WorkspaceClient   # official Databricks client; how we talk to the workspace

# The Statement Execution API takes typed SDK objects, not plain dicts.
# StatementParameterListItem = one bound SQL parameter.
# StatementState = the enum used to check whether the statement actually succeeded.
from databricks.sdk.service.sql import StatementParameterListItem, StatementState


# ---------------------------------------------------------------------------
# APP INITIALIZATION
# ---------------------------------------------------------------------------
# Standard FastAPI app object. The title shows up in the auto-generated
# Swagger docs at <your-app-url>/docs, which is a free way to test the
# endpoint from a browser without writing a client.
app = FastAPI(title="Policy Copilot Ingestion API")

# WorkspaceClient() with NO arguments is the important bit. Databricks Apps
# injects DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (the app's own
# service principal, i.e. its machine identity) into the environment at
# runtime. The SDK finds those automatically. This is why there is no token,
# no username, and no connection string anywhere in this file.
w = WorkspaceClient()


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# This env var is populated by app.yaml, which maps it to the SQL warehouse
# you attach as an app "resource" in the Apps UI. Using os.environ[...] rather
# than os.getenv(...) is deliberate: if the resource is missing or misnamed,
# the app crashes loudly at startup instead of failing with a confusing
# NoneType error on the first request.
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

# Fully qualified Unity Catalog name: catalog.schema.table
TABLE = "policy_copilot.bronze.raw_documents"


# ---------------------------------------------------------------------------
# REQUEST SCHEMA
# ---------------------------------------------------------------------------
# Pydantic model defining what a valid POST body looks like. FastAPI validates
# every incoming request against this automatically and returns a 422 with a
# useful error message if it does not match, so bad data never reaches bronze.
class DocumentIn(BaseModel):
    # The regex pattern is the guardrail that keeps your two corpora clean.
    # A typo like "SORN" or "nist" gets rejected at the door rather than
    # silently creating a third doc_type you have to clean up later.
    doc_type: str = Field(..., pattern="^(sorn|nist_control)$")

    source_id: str        # e.g. "DHS/ALL-024" for a SORN, or "AC-2" for a NIST control
    raw_text: str         # the full document or control text, unmodified
    metadata: dict = {}   # anything else: title, agency, url, effective_date. Defaults to empty.


# ---------------------------------------------------------------------------
# HEALTH CHECK ENDPOINT
# ---------------------------------------------------------------------------
# Not strictly required, but very useful: hit this first after deploying to
# confirm the app is running before you debug anything about SQL or permissions.
@app.get("/health")
def health():
    return {"status": "healthy"}


# ---------------------------------------------------------------------------
# INGESTION ENDPOINT
# ---------------------------------------------------------------------------
@app.post("/ingest")
def ingest_document(doc: DocumentIn):
    # --- Generate server-side fields -------------------------------------
    # The client does not supply these. Generating them here means every row
    # is traceable (ingestion_id) and ordered (ingested_at) regardless of what
    # the caller sent.
    ingestion_id = str(uuid.uuid4())
    # NOTE: deliberately NOT isoformat(). isoformat() produces
    # "2026-07-31T14:21:45.123456+00:00", and the T separator plus offset is
    # fragile to cast into a Databricks TIMESTAMP. This format is unambiguous.
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # --- Build the MERGE statement ---------------------------------------
    # MERGE is Delta's upsert: match incoming rows against existing ones on a
    # key, then UPDATE where they match and INSERT where they don't. This is
    # what makes the endpoint idempotent. POSTing the same document twice
    # updates the existing row instead of creating a duplicate, so a retry
    # after a timeout is now harmless.
    #
    # The business key is (doc_type, source_id), NOT ingestion_id. ingestion_id
    # is a fresh UUID on every request, so merging on it would match nothing
    # and every POST would insert, which is the duplicate behavior we are
    # fixing. The key has to be what identifies the *document*, not the request.
    #
    # `SELECT ... ` with no FROM builds a one-row source relation from the bound
    # parameters, which is what MERGE needs on the USING side.
    #
    # Note the :named placeholders. We are NOT f-stringing the document text
    # into the SQL. Legal and policy text is full of apostrophes and quotes
    # that would break a naive string-built query, and string interpolation is
    # a SQL injection risk. Parameter binding handles both.
    statement = f"""
        MERGE INTO {TABLE} AS target
        USING (
            SELECT
                :ingestion_id AS ingestion_id,
                :doc_type     AS doc_type,
                :source_id    AS source_id,
                :raw_text     AS raw_text,
                :metadata     AS metadata,
                :ingested_at  AS ingested_at
        ) AS source
        ON  target.doc_type  = source.doc_type
        AND target.source_id = source.source_id
        WHEN MATCHED THEN UPDATE SET
            -- ingestion_id is refreshed so it always points at the request
            -- that produced the current contents of the row.
            target.ingestion_id = source.ingestion_id,
            target.raw_text     = source.raw_text,
            target.metadata     = source.metadata,
            target.ingested_at  = source.ingested_at
        WHEN NOT MATCHED THEN INSERT (
            ingestion_id, doc_type, source_id, raw_text, metadata, ingested_at
        ) VALUES (
            source.ingestion_id, source.doc_type, source.source_id,
            source.raw_text, source.metadata, source.ingested_at
        )
    """

    # --- Bind the parameters ---------------------------------------------
    # These must be StatementParameterListItem objects, not plain dicts. The
    # SDK serializes each one by calling .as_dict() on it, so passing a raw
    # dict fails with "'dict' object has no attribute 'as_dict'".
    #
    # metadata is json.dumps'd because the bronze column is STRING; you can
    # parse it back out with Spark's from_json in the silver layer next week.
    parameters = [
        StatementParameterListItem(name="ingestion_id", value=ingestion_id, type="STRING"),
        StatementParameterListItem(name="doc_type", value=doc.doc_type, type="STRING"),
        StatementParameterListItem(name="source_id", value=doc.source_id, type="STRING"),
        StatementParameterListItem(name="raw_text", value=doc.raw_text, type="STRING"),
        StatementParameterListItem(name="metadata", value=json.dumps(doc.metadata), type="STRING"),
        StatementParameterListItem(name="ingested_at", value=ingested_at, type="TIMESTAMP"),
    ]

    # --- Execute against the SQL warehouse --------------------------------
    # This is the Statement Execution API: it runs SQL on the warehouse you
    # attached as a resource. Conceptually the same as running a query through
    # psycopg2 against Postgres, except you target a warehouse_id instead of
    # a host/port, and auth is handled by the service principal.
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=statement,
            parameters=parameters,
            # Wait inline for up to 50s (the API maximum) before falling back
            # to async. Cuts down how often we need to poll below.
            wait_timeout="50s",
        )
    except Exception as e:
        # Surfacing the raw error is intentional while you are building. The
        # two failures you are most likely to hit are (a) the service principal
        # lacking MODIFY on the table and (b) the catalog/schema not existing
        # yet, and both produce readable messages here.
        raise HTTPException(status_code=500, detail=str(e))

    # --- Poll until the statement reaches a terminal state ----------------
    # Two subtleties, both worth knowing:
    #
    # 1. execute_statement does NOT raise when the SQL itself fails. It returns
    #    normally with the failure recorded in resp.status. Without an explicit
    #    check, a permissions error would return {"status": "ok"} and you would
    #    think the row landed.
    #
    # 2. It also returns normally while the statement is still running. A
    #    serverless warehouse that has auto-stopped needs to cold start, which
    #    can take a minute or more, and PENDING/RUNNING means "not done yet,"
    #    not "failed." So we poll rather than treating non-SUCCEEDED as an error.
    TERMINAL = {
        StatementState.SUCCEEDED,
        StatementState.FAILED,
        StatementState.CANCELED,
        StatementState.CLOSED,
    }
    deadline = time.monotonic() + 300  # generous: cold starts are slow on Free Edition

    while resp.status is not None and resp.status.state not in TERMINAL:
        if time.monotonic() > deadline:
            raise HTTPException(
                status_code=504,
                detail="Timed out waiting for the warehouse. It may still be starting; try again.",
            )
        time.sleep(3)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status is None or resp.status.state != StatementState.SUCCEEDED:
        detail = str(resp.status.error) if resp.status and resp.status.error else str(resp.status)
        raise HTTPException(status_code=500, detail=f"Statement did not succeed: {detail}")

    # --- Return the receipt -----------------------------------------------
    # MERGE returns a result row with num_inserted_rows / num_updated_rows, so
    # we can tell you whether this POST created a new document or refreshed an
    # existing one. Useful for confirming idempotency actually works: POST the
    # same doc twice and the second response should say "updated".
    action = "unknown"
    try:
        data_array = resp.result.data_array if resp.result else None
        if data_array and resp.manifest and resp.manifest.schema:
            cols = [c.name for c in resp.manifest.schema.columns]
            row = dict(zip(cols, data_array[0]))
            inserted = int(row.get("num_inserted_rows", 0) or 0)
            updated = int(row.get("num_updated_rows", 0) or 0)
            action = "inserted" if inserted else ("updated" if updated else "no_change")
    except Exception:
        # Parsing the receipt is a nicety, not the job. If the shape of the
        # response changes, the write still succeeded, so do not fail here.
        pass

    return {
        "status": "ok",
        "action": action,
        "ingestion_id": ingestion_id,
        "doc_type": doc.doc_type,
        "source_id": doc.source_id,
    }