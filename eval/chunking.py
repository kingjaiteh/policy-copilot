"""Python mirror of the dbt chunking models, for offline evaluation.

WHAT THIS IS, AND WHAT IT IS NOT

This module reimplements, in Python, exactly what these three files do in SQL:

    dbt/macros/sorn_chunk_text.sql        the embedded text and the oversize test
    dbt/models/silver/sorn_sections_split.sql   the splitter
    dbt/models/gold/document_chunks.sql   assembly, tagging, exclusions

It exists so the A/B can be run, and the chunker inspected, WITHOUT a warehouse
round trip -- which matters because deciding `embed_max_chars` by pushing a
change, rebuilding gold, re-embedding the index and querying it is a slow and
expensive way to answer "how many sections would that even split?"

IT IS A MIRROR, SO IT CAN DRIFT. The SQL is the source of truth; this is a
convenience. `python eval/chunking.py --check` prints the corpus statistics the
SQL should reproduce, so the two can be compared deliberately rather than
assumed equal. If you change one, change the other and re-run that check.

The mirroring is exact in the places where it is easy to get wrong, and those
places are commented inline: separator handling, the 1-based/0-based offset in
the overlap snap, and enumeration happening BEFORE the blank filter (Spark's
posexplode numbers the original array, so filtering first would shift every
position).
"""

from __future__ import annotations

import json
import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import sorn_parser  # noqa: E402


# --- Parameters -------------------------------------------------------------
# These MUST match the vars in dbt/dbt_project.yml. They are duplicated rather
# than parsed out of the YAML so this module has no dbt dependency; --check
# reports them so a mismatch is visible rather than silent.
EMBED_MAX_CHARS = 2000
SPLIT_TARGET_CHARS = 1200
SPLIT_OVERLAP_CHARS = 200

# Sections gold drops before indexing. See the WHERE clause in
# gold/document_chunks.sql for why each one is here.
EXCLUDED_SECTION_KEYS = {
    "routine_uses",              # re-emitted item by item as its own chunks
    "system_name_and_number",    # metadata restating a typed column
    "system_name",
    "security_classification",
}

# Mirrors '(?<=[a-z0-9)\]][.?!])\s+' in the SQL. The lookbehind demands a
# lowercase letter, digit or closing bracket before the stop, which is what
# keeps "U.S. Customs" and "Docket No. 5" from splitting.
SENTENCE_BOUNDARY = re.compile(r"(?<=[a-z0-9)\]][.?!])\s+")


@dataclass(frozen=True)
class Chunk:
    """One row of gold.document_chunks."""

    chunk_id: str
    doc_type: str
    chunk_type: str
    chunk_strategy: str
    source_id: str
    section_key: str | None
    chunk_text: str

    @property
    def target(self) -> tuple[str, str | None]:
        """The arm-independent grading key.

        chunk_id differs between arms by construction (':sec:purpose' vs
        ':sec:purpose:p0'), so ground truth is expressed as
        (source_id, section_key) and this is what it matches against.
        """
        return (self.source_id, self.section_key)


# --- The chunk text and the oversize test -----------------------------------

def chunk_text_for(document_title: str, agency: str | None, heading: str | None,
                   body: str) -> str:
    """Mirror of the sorn_chunk_text macro.

    concat_ws SKIPS null arguments rather than propagating them, hence the
    filter rather than a plain join.
    """
    head = f"{document_title} ({agency or 'Unknown agency'})"
    parts = [p for p in (head, heading, body) if p is not None]
    return "\n\n".join(parts)


def is_oversized(document_title: str, agency: str | None, heading: str | None,
                 body: str) -> bool:
    """Mirror of the sorn_chunk_is_oversized macro.

    Measured on the ASSEMBLED text, never on the bare section: the title and
    heading prefix is worth 100-250 characters and decides the classification
    of everything sitting near the threshold.
    """
    return len(chunk_text_for(document_title, agency, heading, body)) > EMBED_MAX_CHARS


# --- The splitter -----------------------------------------------------------

def atomize(text: str, cap: int = SPLIT_TARGET_CHARS) -> list[str]:
    """Three-tier split: lines, then sentences, then a hard character cut.

    Returns atoms each carrying their own LEADING separator character -- '\\n'
    when the atom starts a new source line, ' ' when it continues one. Baking
    the separator in is what lets the packer be a plain concat while still
    reproducing the original line structure.
    """
    atoms: list[str] = []

    # enumerate() BEFORE the blank check, because Spark's posexplode numbers
    # the original array and the `where` filters afterwards. Filtering first
    # would renumber the lines and change which atoms get a '\n' separator.
    for line_pos, line in enumerate(text.split("\n")):
        if not line.strip():
            continue

        for sent_pos, sent in enumerate(SENTENCE_BOUNDARY.split(line)):
            if not sent.strip():
                continue

            # sequence(0, floor((len-1)/cap)) yields [0] for anything already
            # under the cap, so the common case passes through untouched.
            n_pieces = int(math.floor((len(sent) - 1) / cap)) + 1
            for piece_idx in range(n_pieces):
                piece = sent[piece_idx * cap:(piece_idx + 1) * cap]
                starts_line = sent_pos == 0 and piece_idx == 0 and line_pos > 0
                atoms.append(("\n" if starts_line else " ") + piece)

    return atoms


def pack(atoms: list[str], target: int = SPLIT_TARGET_CHARS) -> list[str]:
    """Greedy pack. Mirror of the `aggregate(...)` fold in the SQL.

    Note `atom[1:]` rather than `atom.strip()`: the SQL uses substring(atom, 2)
    to drop exactly the one separator character, because Spark's trim() would
    leave a leading newline in place.
    """
    parts: list[str] = []
    cur = ""

    for atom in atoms:
        if cur == "":
            cur = atom[1:]
        elif len(cur) + len(atom) <= target:
            cur = cur + atom
        else:
            parts.append(cur)
            cur = atom[1:]

    if cur != "":
        parts.append(cur)

    return parts


def apply_overlap(parts: list[str], overlap: int = SPLIT_OVERLAP_CHARS) -> list[str]:
    """Prepend a tail of the previous part to each part after the first.

    Reads the PRE-overlap text of the previous part (the SQL's lag() sees the
    same), so overlaps never compound down a long section.
    """
    out: list[str] = []

    for i, part in enumerate(parts):
        if i == 0:
            out.append(part)
            continue

        tail = parts[i - 1][-overlap:]

        # Snap forward to a word boundary. SQL: instr() is 1-based and returns
        # 0 when absent, so `substring(t, instr(t,' ') + 1)` starts at 0-based
        # index instr, which for a 0-based find() of i is i + 1.
        space = tail.find(" ")
        if space >= 0:
            tail = tail[space + 1:]

        out.append(tail + "\n" + part)

    return out


def split_section(text: str) -> list[str]:
    """Full splitter pipeline for one oversized section."""
    return apply_overlap(pack(atomize(text)))


# --- Corpus assembly --------------------------------------------------------

@dataclass
class Corpus:
    chunks: list[Chunk] = field(default_factory=list)

    def arm(self, name: str) -> list[Chunk]:
        """The chunks visible to one arm of the A/B.

        Mirrors the filters documented in gold/document_chunks.sql:
          arm A  chunk_strategy != 'sub_split'
          arm B  chunk_strategy != 'oversized_whole'
        """
        if name == "whole":
            return [c for c in self.chunks if c.chunk_strategy != "sub_split"]
        if name == "split":
            return [c for c in self.chunks if c.chunk_strategy != "oversized_whole"]
        raise ValueError(f"unknown arm {name!r}; expected 'whole' or 'split'")


def build_corpus(sorn_dir: Path | None = None,
                 nist_payloads: Path | None = None) -> Corpus:
    """Build every chunk both arms can see, from the local source documents."""
    sorn_dir = sorn_dir or (REPO_ROOT / "data" / "sorn_docx")
    nist_payloads = nist_payloads or (REPO_ROOT / "scripts" / "nist_payloads.json")

    chunks: list[Chunk] = []

    for path in sorted(sorn_dir.glob("*.docx")):
        record = sorn_parser.parse_sorn(path)
        # stg_sorn keeps anything from_json could match, which is 'ok' and
        # 'partial'; only an unparseable document is dropped.
        if record["parse_status"] not in ("ok", "partial"):
            continue

        meta = record["metadata"]
        source_id = record["source_id"]
        title = meta.get("title") or ""
        agency = meta.get("agency")

        for section in record["sections"]:
            key = section["section_key"]
            if key in EXCLUDED_SECTION_KEYS:
                continue
            body = section["text"]
            if not body or not body.strip():
                continue

            heading = section["heading"]

            if not is_oversized(title, agency, heading, body):
                chunks.append(Chunk(
                    chunk_id=f"sorn:{source_id}:sec:{key}",
                    doc_type="sorn", chunk_type="section",
                    chunk_strategy="standard",
                    source_id=source_id, section_key=key,
                    chunk_text=chunk_text_for(title, agency, heading, body),
                ))
                continue

            # Arm A: the section kept whole.
            chunks.append(Chunk(
                chunk_id=f"sorn:{source_id}:sec:{key}",
                doc_type="sorn", chunk_type="section",
                chunk_strategy="oversized_whole",
                source_id=source_id, section_key=key,
                chunk_text=chunk_text_for(title, agency, heading, body),
            ))

            # Arm B: the same section as overlapping parts.
            parts = split_section(body)
            for part_no, part_text in enumerate(parts):
                part_heading = f"{heading} (part {part_no + 1} of {len(parts)})"
                chunks.append(Chunk(
                    chunk_id=f"sorn:{source_id}:sec:{key}:p{part_no}",
                    doc_type="sorn", chunk_type="section",
                    chunk_strategy="sub_split",
                    source_id=source_id, section_key=key,
                    chunk_text=chunk_text_for(title, agency, part_heading, part_text),
                ))

        # Routine uses are already item-level and always 'standard'.
        for item in record["routine_uses"]:
            label = item["label"]
            heading = f"Routine Use {label}"

            # Mirror of routine_use_full_text in silver/sorn_routine_uses.sql,
            # which FOLDS subitems into the parent. Reading item['text'] alone
            # understates the length by up to ~900 characters and is what hid
            # the three over-ceiling routine uses from this check.
            body = item["text"]
            subitems = item.get("subitems") or []
            if subitems:
                body = body + "\n" + "\n".join(
                    f"{s['label']} {s['text']}" for s in subitems)

            if not is_oversized(title, agency, heading, body):
                chunks.append(Chunk(
                    chunk_id=f"sorn:{source_id}:ru:{label}",
                    doc_type="sorn", chunk_type="routine_use",
                    chunk_strategy="standard",
                    source_id=source_id, section_key="routine_uses",
                    chunk_text=chunk_text_for(title, agency, heading, body),
                ))
                continue

            parts = split_section(body)
            for part_no, part_text in enumerate(parts):
                part_heading = f"{heading} (part {part_no + 1} of {len(parts)})"
                chunks.append(Chunk(
                    chunk_id=f"sorn:{source_id}:ru:{label}:p{part_no}",
                    doc_type="sorn", chunk_type="routine_use",
                    chunk_strategy="standard",
                    source_id=source_id, section_key="routine_uses",
                    chunk_text=chunk_text_for(title, agency, part_heading, part_text),
                ))

    # NIST controls are shared ballast: identical in both arms, so they cannot
    # be the cause of any difference between them, but they still compete for
    # top-k slots the way they will in production.
    #
    # The 27 controls over the ceiling are split, but tagged 'standard' like
    # the rest -- splitting them is a truncation fix that applies in both arms,
    # not a second experimental variable. See silver/nist_controls_split.sql.
    if nist_payloads.exists():
        for control in json.loads(nist_payloads.read_text(encoding="utf-8")):
            control_id = control["source_id"]
            raw_text = control["raw_text"]
            title = raw_text.split("\n", 1)[0]

            if len(raw_text) <= EMBED_MAX_CHARS:
                chunks.append(Chunk(
                    chunk_id=f"nist:{control_id}",
                    doc_type="nist_control", chunk_type="control",
                    chunk_strategy="standard",
                    source_id=control_id, section_key=None,
                    chunk_text=raw_text,
                ))
                continue

            parts = split_section(raw_text)
            for part_no, part_text in enumerate(parts):
                marker = f"{title} (part {part_no + 1} of {len(parts)})"
                chunks.append(Chunk(
                    chunk_id=f"nist:{control_id}:p{part_no}",
                    doc_type="nist_control", chunk_type="control",
                    chunk_strategy="standard",
                    source_id=control_id, section_key=None,
                    chunk_text=f"{marker}\n\n{part_text}",
                ))

    return Corpus(chunks=chunks)


# --- Self-check -------------------------------------------------------------

def check() -> int:
    """Print what the chunker does to this corpus. Compare against the SQL."""
    corpus = build_corpus()

    by_strategy: dict[str, int] = {}
    for chunk in corpus.chunks:
        by_strategy[chunk.chunk_strategy] = by_strategy.get(chunk.chunk_strategy, 0) + 1

    print(f"embed_max_chars={EMBED_MAX_CHARS} "
          f"split_target_chars={SPLIT_TARGET_CHARS} "
          f"split_overlap_chars={SPLIT_OVERLAP_CHARS}")
    print()
    print("chunks by strategy (should match gold.document_chunks):")
    for name in ("standard", "oversized_whole", "sub_split"):
        print(f"  {name:16} {by_strategy.get(name, 0):5}")
    print(f"  {'TOTAL':16} {len(corpus.chunks):5}")
    print()
    print(f"arm 'whole' (A): {len(corpus.arm('whole')):5} chunks")
    print(f"arm 'split' (B): {len(corpus.arm('split')):5} chunks")
    print()

    oversized = [c for c in corpus.chunks if c.chunk_strategy == "oversized_whole"]
    parts = [c for c in corpus.chunks if c.chunk_strategy == "sub_split"]
    per_section: dict[tuple[str, str | None], int] = {}
    for part in parts:
        per_section[part.target] = per_section.get(part.target, 0) + 1

    failures = 0

    # The invariant the dbt singular test asserts: every oversized section must
    # actually have been split into two or more parts.
    unsplit = sorted(t for t in (c.target for c in oversized)
                     if per_section.get(t, 0) < 2)
    if unsplit:
        failures += 1
        print(f"FAIL  {len(unsplit)} oversized section(s) produced fewer than 2 parts")
        for source_id, key in unsplit[:10]:
            print(f"        {source_id} {key} -> {per_section.get((source_id, key), 0)}")
    else:
        print(f"ok    all {len(oversized)} oversized sections split into >= 2 parts "
              f"({len(parts)} parts total, "
              f"{len(parts) / max(len(oversized), 1):.1f} avg)")

    # Coverage in both directions, mirroring assert_sub_split_covers_oversized.
    whole_targets = {c.target for c in oversized}
    part_targets = set(per_section)
    if whole_targets != part_targets:
        failures += 1
        print(f"FAIL  arm coverage mismatch: "
              f"{len(whole_targets - part_targets)} missing parts, "
              f"{len(part_targets - whole_targets)} orphan parts")
    else:
        print("ok    every oversized section has parts and vice versa")

    # The ceiling, exempting the deliberately-unsplit control arm.
    breaches = [c for c in corpus.chunks
                if c.chunk_strategy != "oversized_whole"
                and len(c.chunk_text) > EMBED_MAX_CHARS]
    if breaches:
        failures += 1
        print(f"FAIL  {len(breaches)} chunk(s) exceed embed_max_chars={EMBED_MAX_CHARS}")
        for chunk in sorted(breaches, key=lambda c: -len(c.chunk_text))[:10]:
            print(f"        {len(chunk.chunk_text):6}  {chunk.chunk_id}")
    else:
        longest = max((len(c.chunk_text) for c in corpus.chunks
                       if c.chunk_strategy != "oversized_whole"), default=0)
        print(f"ok    no chunk over the ceiling (longest non-control: {longest})")

    print()
    if failures:
        print(f"{failures} check(s) FAILED")
    else:
        print("all checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(check())
