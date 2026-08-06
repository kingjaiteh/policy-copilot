"""
Fetch NIST SP 800-53 Rev 5 controls and POST them to the ingestion API.

WHY THIS IS EASY: NIST publishes the entire control catalog as machine-readable
JSON in a format called OSCAL (Open Security Controls Assessment Language), a
NIST standard for expressing security controls so tools can consume them
without parsing PDFs. No scraping required.

Source: https://github.com/usnistgov/oscal-content

Usage:
    pip install requests
    python fetch_nist_controls.py --dry-run            # write payloads to disk, POST nothing
    python fetch_nist_controls.py                      # default families, POST to APP_URL
    python fetch_nist_controls.py --families ac au ia  # pick your own
    python fetch_nist_controls.py --all-families       # everything (~1,190 docs)

HOW TO ACTUALLY LOAD THESE: the POST path needs the FastAPI app deployed and
APP_URL set. The SDK path does not, and is the one in use here -- write the
payloads, then hand them to the loader that already batches and MERGEs the
SORN corpus:

    python fetch_nist_controls.py --dry-run
    python load_to_delta.py --payloads nist_payloads.json

Same bronze table, same MERGE on (doc_type, source_id), so re-running either
updates rather than duplicates.
"""

import argparse
import json
import re
import sys
from pathlib import Path

import requests

# --- Same URL you set in post_sample_docs.py -------------------------------
APP_URL = "PASTE_YOUR_APP_URL_HERE"

RAW = "https://raw.githubusercontent.com/usnistgov/oscal-content/main/nist.gov/SP800-53/rev5/json"
CATALOG_URL = f"{RAW}/NIST_SP-800-53_rev5_catalog.json"
BASELINE_URLS = {
    "low": f"{RAW}/NIST_SP-800-53_rev5_LOW-baseline_profile.json",
    "moderate": f"{RAW}/NIST_SP-800-53_rev5_MODERATE-baseline_profile.json",
    "high": f"{RAW}/NIST_SP-800-53_rev5_HIGH-baseline_profile.json",
}

# Default families chosen to pair well with SORNs: access control, audit,
# identification, system protection, plus the two privacy families that
# overlap most directly with Privacy Act obligations.
DEFAULT_FAMILIES = ["ac", "au", "ia", "sc", "pt", "pm"]

CACHE = Path(__file__).resolve().parent / "nist_cache"

# OSCAL embeds organization-defined parameters as {{ insert: param, ac-1_prm_1 }}.
# Left alone these are noise in an embedding. We substitute the parameter's
# human-readable label instead, so the text reads the way the published PDF does.
PARAM_RE = re.compile(r"\{\{\s*insert:\s*param,\s*([^}\s]+)\s*\}\}")


def get_json(url: str) -> dict:
    """Fetch with a local cache so repeated runs don't re-download 10MB."""
    CACHE.mkdir(exist_ok=True)
    path = CACHE / url.rsplit("/", 1)[-1]
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    print(f"downloading {path.name} ...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    path.write_text(resp.text, encoding="utf-8")
    return resp.json()


def collect_params(control: dict) -> dict:
    """Map param id -> readable placeholder, e.g. 'organization-defined personnel'."""
    out = {}
    for p in control.get("params", []):
        label = p.get("label")
        if not label and p.get("select"):
            choices = [c.get("value", c) if isinstance(c, dict) else c
                       for c in p["select"].get("choice", [])]
            label = " | ".join(str(c) for c in choices)
        out[p["id"]] = f"[{label}]" if label else f"[{p['id']}]"
    return out


def resolve(text: str, params: dict) -> str:
    return PARAM_RE.sub(lambda m: params.get(m.group(1), f"[{m.group(1)}]"), text or "")


def flatten_parts(parts, params, depth=0, keep=("statement", "guidance", "item")):
    """
    Recursively turn OSCAL 'parts' into readable text.

    We keep the statement (the control requirement itself), its nested items,
    and the discussion/guidance. We deliberately DROP assessment-objective and
    assessment-method parts: those come from SP 800-53A and are about how an
    auditor tests the control, which roughly triples the text volume and pulls
    retrieval toward audit procedures rather than the requirement itself.
    """
    lines = []
    for part in parts or []:
        name = part.get("name")
        if name not in keep:
            continue
        label = next((p["value"] for p in part.get("props", []) if p["name"] == "label"), "")
        prose = resolve(part.get("prose", ""), params)
        if prose or label:
            indent = "  " * depth
            prefix = f"{label} " if label else ""
            if name == "guidance" and depth == 0:
                lines.append("\nDiscussion:")
            lines.append(f"{indent}{prefix}{prose}".rstrip())
        lines.extend(flatten_parts(part.get("parts"), params, depth + 1, keep))
    return lines


def build_doc(control, family_id, family_title, baselines, parent=None):
    """Turn one OSCAL control (or enhancement) into an /ingest payload."""
    params = collect_params(control)

    # Controls carry several 'label' props; the non-classed one is the
    # familiar human format ("AC-2", "AC-2(1)").
    label = next(
        (p["value"] for p in control.get("props", [])
         if p["name"] == "label" and "class" not in p),
        control["id"].upper(),
    )

    body = flatten_parts(control.get("parts"), params)
    raw_text = f"{label} {control['title']}\n\n" + "\n".join(body).strip()

    related = [l["href"].lstrip("#").upper()
               for l in control.get("links", []) if l.get("rel") == "related"]

    return {
        "doc_type": "nist_control",
        "source_id": label,
        "raw_text": raw_text,
        "metadata": {
            "title": control["title"],
            "control_id": control["id"],
            "family_id": family_id.upper(),
            "family": family_title,
            "is_enhancement": parent is not None,
            "parent_control": parent,
            "related_controls": related,
            # Which FIPS-199 impact baselines include this control. This is what
            # lets the copilot answer "what applies at moderate?"
            "baselines": [b for b, ids in baselines.items() if control["id"] in ids],
            "source": "NIST SP 800-53 Rev 5 (OSCAL)",
            "source_url": CATALOG_URL,
            "char_count": len(raw_text),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--families", nargs="*", default=None,
                    help=f"family ids, default: {' '.join(DEFAULT_FAMILIES)}")
    ap.add_argument("--all-families", action="store_true")
    ap.add_argument("--no-enhancements", action="store_true",
                    help="skip control enhancements like AC-2(1)")
    ap.add_argument("--dry-run", action="store_true",
                    help="write payloads to nist_payloads.json instead of POSTing")
    args = ap.parse_args()

    catalog = get_json(CATALOG_URL)["catalog"]

    baselines = {}
    for name, url in BASELINE_URLS.items():
        prof = get_json(url)["profile"]
        ids = set()
        for imp in prof.get("imports", []):
            for inc in imp.get("include-controls", []):
                ids.update(inc.get("with-ids", []))
        baselines[name] = ids

    wanted = None if args.all_families else set(
        f.lower() for f in (args.families or DEFAULT_FAMILIES)
    )

    docs = []
    for group in catalog.get("groups", []):
        fid, ftitle = group["id"], group["title"]
        if wanted is not None and fid.lower() not in wanted:
            continue
        for control in group.get("controls", []):
            docs.append(build_doc(control, fid, ftitle, baselines))
            if not args.no_enhancements:
                for enh in control.get("controls", []):
                    docs.append(build_doc(enh, fid, ftitle, baselines, parent=control["id"].upper()))

    if not docs:
        sys.exit("No controls matched. Check your --families values against the catalog group ids.")

    print(f"built {len(docs)} documents")

    if args.dry_run:
        out = Path(__file__).resolve().parent / "nist_payloads.json"
        out.write_text(json.dumps(docs, indent=2), encoding="utf-8")
        print(f"wrote {out}")
        print(f"\nload it with:  python load_to_delta.py --payloads {out.name}")
        print("\n--- sample ---")
        print(docs[0]["raw_text"][:700])
        return

    if APP_URL.startswith("PASTE"):
        sys.exit(
            "APP_URL is not set, so there is nowhere to POST.\n"
            "The app is not deployed; use the SDK path instead:\n"
            "  python fetch_nist_controls.py --dry-run\n"
            "  python load_to_delta.py --payloads nist_payloads.json"
        )

    ok = failed = 0
    for i, doc in enumerate(docs, 1):
        try:
            # Long timeout: the first request may cold-start the warehouse.
            r = requests.post(f"{APP_URL}/ingest", json=doc, timeout=310)
            if r.status_code == 200:
                ok += 1
                print(f"[{i}/{len(docs)}] {doc['source_id']} {r.json().get('action')}")
            else:
                failed += 1
                print(f"[{i}/{len(docs)}] {doc['source_id']} FAILED {r.status_code} {r.text[:200]}")
        except Exception as e:
            failed += 1
            print(f"[{i}/{len(docs)}] {doc['source_id']} ERROR {e}")

    print(f"\ndone: {ok} ok, {failed} failed")
    # Safe to re-run: /ingest MERGEs on (doc_type, source_id), so retrying
    # failures will update rather than duplicate.


if __name__ == "__main__":
    main()
