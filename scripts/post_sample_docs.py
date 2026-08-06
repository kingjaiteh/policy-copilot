"""
Parse the local SORN .docx files and POST them to the ingestion app.

Usage
-----
    python post_sample_docs.py            # parse + post every .docx here
    python post_sample_docs.py --dry-run  # parse + print, no network calls
    python post_sample_docs.py --dir path/to/docs

The parsing lives in sorn_parser.py. This script only decides what to send and
reports what happened.

The bronze table's ``metadata`` column is a STRING holding JSON, so the entire
structured parse (typed metadata + sections + routine uses) rides along in that
one field. That keeps app.py and the bronze DDL unchanged while still landing
everything the silver layer needs: sections become chunks for retrieval, and
routine uses become the labeled units for classifying data-sharing patterns.
Pull them apart later with from_json + explode.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

import sorn_parser

APP_URL = "https://dbc-1d037021-8869.cloud.databricks.com"  # <-- update after deploy

# The corpus lives outside the repo (data/ is gitignored). Resolved from this
# file rather than the CWD so the script works from anywhere.
DEFAULT_DOCS_DIR = Path(__file__).resolve().parent.parent / "data" / "sorn_docx"


def to_payload(record: dict) -> dict:
    """Shape a parsed SORN into the app's DocumentIn body.

    raw_text stays the full document text. Everything structured goes into
    metadata, which app.py json.dumps into the STRING column.
    """
    return {
        "doc_type": "sorn",
        "source_id": record["source_id"],
        "raw_text": record["raw_text"],
        "metadata": {
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
        },
    }


def post_doc(payload: dict) -> None:
    response = requests.post(f"{APP_URL}/ingest", json=payload, timeout=120)
    try:
        body = response.json()
    except ValueError:
        body = response.text[:200]
    print(f"  POST {payload['source_id']:<20} -> {response.status_code} {body}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=str(DEFAULT_DOCS_DIR),
                        help="directory of .docx files")
    parser.add_argument("--dry-run", action="store_true", help="parse only, do not POST")
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="also post documents whose body text failed to capture",
    )
    args = parser.parse_args()

    records = sorn_parser.parse_directory(args.dir)
    if not records:
        print(f"No .docx files found in {Path(args.dir).resolve()}")
        return

    print(f"Parsed {len(records)} document(s) from {Path(args.dir).resolve()}\n")

    posted = skipped = 0
    for record in records:
        print(
            f"{record['source_id']:<20} {record['parse_status']:<8} "
            f"chars={record['char_count']:<6} sections={len(record['sections']):<3} "
            f"routine_uses={len(record['routine_uses']):<3} {record['source_file']}"
        )
        if record["quality_flags"]:
            print(f"  flags: {', '.join(record['quality_flags'])}")

        # An empty capture would land a row that looks ingested but carries no
        # content, which is worse than a visible gap. Skipped unless asked for.
        if record["parse_status"] == "empty" and not args.include_empty:
            print("  skipped: no body text recovered (use --include-empty to post anyway)")
            skipped += 1
            continue

        payload = to_payload(record)
        if args.dry_run:
            print(f"  dry-run: metadata {len(json.dumps(payload['metadata'])):,} bytes")
        else:
            post_doc(payload)
        posted += 1

    print(f"\n{posted} posted, {skipped} skipped.")


if __name__ == "__main__":
    main()
