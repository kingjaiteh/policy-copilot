"""
SORN (System of Records Notice) parser.

Turns a Privacy Act SORN .docx into a structured record suitable for
(a) landing in the bronze Delta table, and (b) downstream agentic retrieval
and ML classification of data-sharing patterns.

Why this module exists in this shape
------------------------------------
The .docx files in this corpus are not authored Word documents. They are
web/plain-text captures pasted into Word, so *all* of the body text lives in a
single <w:p> containing a single <w:r>. There are no headings, no styles, and
no paragraph structure to lean on. What *is* there is hundreds of <w:br/>
elements holding the original line structure. A naive ".//w:t" extraction drops
those breaks and glues words together across line boundaries ("categoriesof",
"theDepartment"), which is what makes the raw text look corrupted. Walking the
XML in document order and emitting a newline per <w:br/> recovers the lines
exactly, and everything else follows from that.

Two SORN layouts appear in the corpus and are harmonized onto one set of
section keys:

  * ``pre_2016``       - GPO plain-text capture, Title-case headings
                         ("System name:", "Routine uses of records ...:"),
                         hard-wrapped at ~65-72 chars.
  * ``omb_a108_2016``  - federalregister.gov capture, ALL-CAPS headings
                         ("SYSTEM NAME AND NUMBER:"), one line per paragraph.

The old layout nests storage/retrieval/retention/safeguards under a combined
"Policies and practices for storing, retrieving, ..." container, while the new
layout promotes them to top-level sections. Mapping both onto the same keys is
the whole point: it lets a model or an agent compare a 2014 notice against a
2017 one without caring which template the agency used that year.

No network access, no Databricks imports. Pure parsing so it can be unit
tested and reused inside a Databricks notebook as easily as from a script.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable

# Bumped whenever extraction output changes. Stored on every row so you can
# tell which parser produced a record and re-run selectively after a fix.
PARSER_VERSION = "1.0.0"

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


# ---------------------------------------------------------------------------
# 1. DOCX -> LINES
# ---------------------------------------------------------------------------
def extract_docx_lines(docx_path: str | Path) -> list[str]:
    """Return the document's text as an ordered list of lines.

    Walks word/document.xml in document order rather than collecting <w:t>
    nodes, because the line structure in these files is carried entirely by
    <w:br/>. Dropping those breaks is what glues words together.

    Measured on this corpus, which is what makes the point concrete: each file
    has 2-3 <w:p> elements but 216-549 <w:br/> elements. A paragraph-based
    extraction therefore returns the whole notice as one 30,000-character
    string, and every former line boundary silently disappears -- the space
    that ended the line goes with it, producing "categoriesof" and
    "theDepartment". Those look like a corrupt download; they are not. Emitting
    a newline per <w:br/> recovers all 549 lines exactly as captured.

    <w:tab/> becomes a literal tab for the same reason: it is real horizontal
    whitespace, and dropping it would glue a label to its value.
    """
    with zipfile.ZipFile(Path(docx_path)) as archive:
        xml_bytes = archive.read("word/document.xml")

    root = ET.fromstring(xml_bytes)
    lines: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        lines.append("".join(buffer))
        buffer.clear()

    for element in root.iter():
        tag = element.tag
        if tag == W + "t":
            buffer.append(element.text or "")
        elif tag == W + "tab":
            buffer.append("\t")
        elif tag in (W + "br", W + "cr"):
            flush()
        elif tag == W + "p" and buffer:
            # Paragraph boundary also ends a line.
            flush()

    if buffer:
        flush()

    return lines


# ---------------------------------------------------------------------------
# 2. LINES -> REFLOWED TEXT
# ---------------------------------------------------------------------------
def looks_hard_wrapped(lines: Iterable[str]) -> bool:
    """True when lines are fixed-width wraps rather than real paragraphs.

    This decides how ``reflow`` rejoins lines, and getting it wrong wrecks the
    text either way: join wrapped lines with newlines and every sentence stays
    shredded into 65-character fragments; join paragraph lines with spaces and
    the whole notice collapses into one unsearchable blob.

    The two capture sources are cleanly separable on two measurements, taken
    from this corpus:

        GPO plain text (pre-2016)   median line 64, max 74, 30% end on punct.
        federalregister.gov (2016+) median line 43, max 1179, 50% end on punct.

    A fixed-column wrapper produces lines clustered just under its margin that
    mostly stop mid-sentence, because the break lands wherever the column ran
    out rather than where the sentence did. Real paragraphs have no such
    ceiling -- hence the 1179-character outlier -- and end on punctuation far
    more often. The thresholds below (median 50-85, under 45% terminal) sit in
    the wide gap between those two profiles rather than hugging either one.

    The <20 line guard exists for stub captures such as DHS/USCG-031, which has
    two lines total. There is no distribution to measure, and either reflow
    strategy gives the same answer, so the cheaper default wins.
    """
    body = [line.strip() for line in lines if line.strip()]
    if len(body) < 20:
        return False

    lengths = sorted(len(line) for line in body)
    median = lengths[len(lengths) // 2]
    ends_on_punct = sum(1 for line in body if line.endswith((".", ":", ";", "!", "?")))
    punct_ratio = ends_on_punct / len(body)

    return 50 <= median <= 85 and punct_ratio < 0.45


def reflow(lines: list[str], hard_wrapped: bool) -> str:
    """Join lines back into paragraphs.

    Hard-wrapped text is unwrapped with a single space, because the wrap
    consumed exactly one space when it broke the line -- putting it back is a
    genuine repair, not a guess. Worth noting that GPO plain text breaks only
    at word boundaries and never hyphenates, so there are no split words to
    rejoin; "Security/United" + "States" becomes "Security/United States" and
    nothing needs a dictionary to sort out.

    Paragraph-per-line text is newline-joined, which conveniently puts every
    ALL-CAPS heading at the start of its own line for the section splitter.

    Blank lines end a paragraph in the wrapped case. Without that, the entire
    notice would unwrap into a single line and the paragraph structure the
    Federal Register author intended would be lost for good.
    """
    if not hard_wrapped:
        return "\n".join(line.strip() for line in lines).strip()

    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        current.append(stripped)
    if current:
        paragraphs.append(" ".join(current))

    return "\n".join(paragraphs).strip()


# ---------------------------------------------------------------------------
# 3. TEXT NORMALIZATION
# ---------------------------------------------------------------------------
# GPO plain text marks page transitions inline; federalregister.gov uses its
# own form. Both are noise mid-sentence, but the page numbers themselves are
# worth keeping as metadata, so they are captured before being stripped.
PAGE_MARKER_RE = re.compile(r"\[\[Page\s+([0-9A-Za-z\-]+)\]\]|\(printed page\s+([0-9]+)\)")

# federalregister.gov page chrome that precedes every notice.
FR_BOILERPLATE_RE = re.compile(
    r"Document Headings.*?for more details\.", re.IGNORECASE | re.DOTALL
)
GPO_BOILERPLATE_RE = re.compile(
    r"From the Federal Register Online via the Government Publishing Office\s*\[?\s*"
    r"www\.gpo\.gov\s*\]?",
    re.IGNORECASE,
)
# GPO separates preamble blocks with a rule of dashes. After unwrapping it can
# end up mid-paragraph, so it is stripped anywhere rather than only on its own
# line (otherwise it trails onto the ACTION value).
RULE_LINE_RE = re.compile(r"-{5,}")


def normalize_text(text: str) -> tuple[str, list[str]]:
    """Clean a reflowed document. Returns (clean_text, page_markers)."""
    pages = [m.group(1) or m.group(2) for m in PAGE_MARKER_RE.finditer(text)]
    text = PAGE_MARKER_RE.sub(" ", text)

    text = FR_BOILERPLATE_RE.sub(" ", text)
    text = GPO_BOILERPLATE_RE.sub(" ", text)
    text = RULE_LINE_RE.sub("", text)

    # U+FFFD appears where the capture lost a smart quote. In this corpus every
    # occurrence is a curly double quote around a system name, so folding it to
    # a straight quote is safe and keeps the text tokenizable.
    text = text.replace("�", '"')
    # GPO ASCII quoting: ``quoted'' -> "quoted"
    text = text.replace("``", '"').replace("''", '"')
    text = unicodedata.normalize("NFKC", text)

    # Collapse runs of spaces/tabs but keep paragraph breaks.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]*\n[ \t]*", "\n", text)

    return text.strip(), pages


# ---------------------------------------------------------------------------
# 4. SECTION SPLITTING
# ---------------------------------------------------------------------------
# Canonical key -> heading spellings seen across both SORN templates.
#
# This table is the heart of the module and the reason the corpus is queryable
# at all. Agencies write the same section under different names depending on
# which template year they used and, frankly, on who typed it:
#
#   "System name:"                        (pre-2016)
#   "SYSTEM NAME AND NUMBER:"             (OMB A-108, 2016+)
#   "Categories of records in the system:"       vs "... covered by the system:"
#   "Categories of Records in This System:"      vs "... in the system:"
#   "Record access procedures:"                  vs "Records access procedures:"
#   "PURPOSE(S) OF THE SYSTEM:"                  vs "PURPOSE OF THE SYSTEM:"
#
# Every one of those was found in this corpus, not imagined. Collapsing them
# onto one key per concept is what lets a model or an agent compare a 2008 CBP
# notice against a 2023 FEMA one without first learning six spellings of
# "categories of records".
#
# The old template also nests storage/retrieval/retention/safeguards *under* a
# combined "Policies and practices for storing, retrieving, ..." container,
# while the new one promotes them to top-level sections. Mapping "Storage" and
# "POLICIES AND PRACTICES FOR STORAGE OF RECORDS" to the same key erases that
# structural difference too, which is why `policies_container` exists as its
# own throwaway key: it absorbs the old template's header so its (empty) body
# does not leak into the section that follows.
#
# Order in this list is for human readability only. Matching prefers the
# longest heading, not the first -- see _find_headings.
SECTION_ALIASES: list[tuple[str, list[str]]] = [
    ("system_name_and_number", ["System name and number", "System name"]),
    ("security_classification", ["Security classification"]),
    ("system_location", ["System location"]),
    ("system_manager", ["System manager(s)", "System manager and address", "System manager"]),
    ("authority_for_maintenance", ["Authority for maintenance of the system"]),
    ("purpose", ["Purpose(s) of the system", "Purpose of the system", "Purpose(s)", "Purpose"]),
    ("categories_of_individuals", ["Categories of individuals covered by the system"]),
    (
        "categories_of_records",
        [
            "Categories of records in the system",
            # CBP notices say "covered by" here instead of "in".
            "Categories of records covered by the system",
        ],
    ),
    ("record_source_categories", ["Record source categories"]),
    (
        "routine_uses",
        [
            "Routine uses of records maintained in the system, including categories "
            "of users and purposes of such uses",
            "Routine uses of records maintained in the system, including categories "
            "of users and the purposes of such uses",
            "Routine uses of records maintained in the system",
        ],
    ),
    (
        "disclosure_to_consumer_reporting_agencies",
        ["Disclosure to consumer reporting agencies"],
    ),
    (
        "policies_container",
        [
            "Policies and practices for storing, retrieving, accessing, retaining, "
            "and disposing of records in the system"
        ],
    ),
    ("policies_storage", ["Policies and practices for storage of records", "Storage"]),
    ("policies_retrieval", ["Policies and practices for retrieval of records", "Retrievability"]),
    (
        "policies_retention",
        [
            "Policies and practices for retention and disposal of records",
            "Retention and disposal",
        ],
    ),
    ("safeguards", ["Administrative, technical, and physical safeguards", "Safeguards"]),
    ("record_access_procedures", ["Record access procedures", "Records access procedures"]),
    ("contesting_record_procedures", ["Contesting record procedures"]),
    ("notification_procedures", ["Notification procedures", "Notification procedure"]),
    (
        "exemptions",
        ["Exemptions promulgated for the system", "Exemptions claimed for the system"],
    ),
    ("history", ["History"]),
]

# Federal Register preamble fields. Parsed alongside the SORN body because
# SUMMARY/DATES/ACTION carry the metadata you want to filter on.
FRONT_MATTER_ALIASES: list[tuple[str, list[str]]] = [
    ("fm_agency", ["AGENCY"]),
    ("fm_action", ["ACTION"]),
    ("fm_summary", ["SUMMARY"]),
    ("fm_dates", ["DATES"]),
    ("fm_addresses", ["ADDRESSES"]),
    ("fm_contact", ["FOR FURTHER INFORMATION CONTACT"]),
    ("fm_supplementary", ["SUPPLEMENTARY INFORMATION"]),
]


def _heading_pattern(alias: str) -> re.Pattern[str]:
    """Build a whitespace-tolerant regex for a heading.

    Words are joined with ``\\s*`` rather than ``\\s+`` so a heading still
    matches if a line wrap ate the space between two of its words. Punctuation
    is likewise allowed to carry optional whitespace around it.

    Agencies also swap "the" and "this" freely in these headings ("Categories
    of Records in This System" vs "... in the System"), so that one word is
    matched as an alternation instead of being spelled out as extra aliases.
    """
    parts = []
    for token in alias.split():
        if token.lower() in ("the", "this"):
            parts.append(r"(?:the|this)")
        else:
            parts.append(re.escape(token))
    # Some notices run the heading into the sentence ("Categories of records in
    # this system include:"), so an optional "include(s)" is absorbed into the
    # heading rather than being left to start the body.
    return re.compile(r"\s*".join(parts) + r"(?:\s*includes?)?\s*:", re.IGNORECASE)


_SECTION_PATTERNS = [
    (key, alias, _heading_pattern(alias))
    for key, aliases in SECTION_ALIASES
    for alias in aliases
]
_FRONT_MATTER_PATTERNS = [
    (key, alias, _heading_pattern(alias))
    for key, aliases in FRONT_MATTER_ALIASES
    for alias in aliases
]


def _find_headings(text: str, patterns) -> list[dict]:
    """Locate headings, keeping the longest match when several overlap.

    Longest-wins is not a stylistic preference, it is required for correctness.
    "SAFEGUARDS:" is a literal suffix of "ADMINISTRATIVE, TECHNICAL, AND
    PHYSICAL SAFEGUARDS:", and both are in the alias table because different
    templates use each. Matched naively, DHS/USCG-029 reports the heading twice
    -- once at offset 25371 and again at 25411, exactly 40 characters later,
    which is the length of the longer heading. The second hit then becomes a
    section boundary, truncating the real safeguards body to zero characters.
    The same trap applies to "Purpose" inside "Purpose(s) of the system".

    Sorting by (start, -length) puts the longest candidate at each position
    first, so it is already in `kept` by the time its shorter suffixes are
    tested and they are rejected as overlapping.
    """
    hits: list[dict] = []
    for key, alias, pattern in patterns:
        for match in pattern.finditer(text):
            hits.append(
                {
                    "key": key,
                    "alias": alias,
                    "start": match.start(),
                    "end": match.end(),
                    "heading": match.group(0).strip(),
                }
            )

    # Longest first so the winner is chosen before shorter overlaps are seen.
    hits.sort(key=lambda h: (h["start"], -(h["end"] - h["start"])))

    kept: list[dict] = []
    for hit in hits:
        if any(hit["start"] < k["end"] and hit["end"] > k["start"] for k in kept):
            continue
        kept.append(hit)

    kept.sort(key=lambda h: h["start"])
    return kept


def split_sections(text: str) -> tuple[dict[str, dict], dict[str, str]]:
    """Split a normalized SORN into canonical sections and front matter.

    Returns (sections, front_matter). ``sections`` maps canonical key ->
    {heading, text, ordinal, start, end}. A section's body runs to the start of
    the next heading of *either* kind, so front matter cannot swallow the body.
    """
    section_hits = _find_headings(text, _SECTION_PATTERNS)
    front_hits = _find_headings(text, _FRONT_MATTER_PATTERNS)

    # Front matter always precedes the SORN body. Anything that looks like a
    # front-matter field after the first real section heading is a false
    # positive (e.g. the word "History" inside a paragraph) and is discarded.
    body_start = section_hits[0]["start"] if section_hits else len(text)
    front_hits = [h for h in front_hits if h["start"] < body_start]

    boundaries = sorted(
        [h["start"] for h in section_hits] + [h["start"] for h in front_hits]
    )

    def body_after(hit: dict) -> str:
        following = [b for b in boundaries if b > hit["start"]]
        stop = following[0] if following else len(text)
        return text[hit["end"] : stop].strip()

    front_matter = {hit["key"]: body_after(hit) for hit in front_hits}

    sections: dict[str, dict] = {}
    for ordinal, hit in enumerate(section_hits):
        key = hit["key"]
        body = body_after(hit)
        if key in sections:
            # A heading word repeating later in the document; keep the first
            # (and richer) occurrence rather than overwriting it with a stub.
            if len(body) <= len(sections[key]["text"]):
                continue
        sections[key] = {
            "heading": hit["heading"],
            "text": body,
            "ordinal": ordinal,
            "start": hit["start"],
            "end": hit["end"],
        }

    return sections, front_matter


# ---------------------------------------------------------------------------
# 5. ROUTINE USES  (the data-sharing patterns)
# ---------------------------------------------------------------------------
# Routine uses are the operative disclosure authorities: each lettered item
# says who records may be shared with and for what purpose. This is the unit
# you want for classification, so it gets split out rather than left as prose.
_SUBITEM_RE = re.compile(r"(?:^|(?<=[\s;:.]))(\d{1,2})\.\s+")


def split_routine_uses(section_text: str) -> list[dict]:
    """Split a routine-uses section into individually addressable items.

    This is the payload for the classification work: each lettered item is one
    disclosure authority naming a recipient and a purpose, so splitting them
    turns a wall of prose into ~370 labelable units across this corpus. Leaving
    the section whole would force any model to treat "may be disclosed to DOJ
    for litigation" and "may be disclosed to a congressional office" as one
    undifferentiated blob.

    Labels are found by walking the alphabet in order -- find "A.", then look
    for "B." only after it, and so on -- rather than matching any
    capital-letter-dot. That distinction matters because legal prose is dense
    with false positives: "U.S. Customs", "5 U.S.C. 552a", and initials in
    contact names all match a naive [A-Z]\\. pattern. A sequential walk cannot
    be fooled by them, because at any moment it is searching for exactly one
    specific letter and only ahead of the previous hit. "U.S." can never be
    mistaken for routine use "S." if "S." would have to precede "R.".

    Text before the first label is kept as a PREAMBLE item rather than
    discarded; it carries the 5 U.S.C. 552a(b)(3) framing that governs every
    item beneath it, which is context a classifier should not lose.
    """
    if not section_text:
        return []

    positions: list[tuple[str, int, int]] = []
    cursor = 0
    letter_index = 0
    alphabet = [chr(ord("A") + i) for i in range(26)]

    while letter_index < len(alphabet):
        letter = alphabet[letter_index]
        pattern = re.compile(
            r"(?:^|(?<=[\s\"(]))" + re.escape(letter) + r"\.\s+(?=[A-Z\"(])"
        )
        match = pattern.search(section_text, cursor)
        if not match:
            break
        positions.append((letter, match.start(), match.end()))
        cursor = match.end()
        letter_index += 1

    if not positions:
        return []

    preamble = section_text[: positions[0][1]].strip()

    items: list[dict] = []
    for i, (letter, start, end) in enumerate(positions):
        stop = positions[i + 1][1] if i + 1 < len(positions) else len(section_text)
        body = section_text[end:stop].strip()

        # Numbered sub-conditions ("1. DHS or any component thereof;") qualify
        # the parent disclosure, so they are kept inside the item text and also
        # surfaced separately for finer-grained modeling.
        subitems = []
        sub_positions = list(_SUBITEM_RE.finditer(body))
        for j, sub in enumerate(sub_positions):
            sub_stop = sub_positions[j + 1].start() if j + 1 < len(sub_positions) else len(body)
            subitems.append(
                {"label": sub.group(1), "text": body[sub.end() : sub_stop].strip()}
            )

        items.append(
            {
                "label": letter,
                "text": body,
                "subitems": subitems,
                "char_count": len(body),
            }
        )

    if preamble:
        items.insert(
            0,
            {
                "label": "PREAMBLE",
                "text": preamble,
                "subitems": [],
                "char_count": len(preamble),
            },
        )

    return items


# ---------------------------------------------------------------------------
# 6. METADATA
# ---------------------------------------------------------------------------
# Systems can be jointly owned, so the prefix may carry several components:
# "DHS/USCG-029" but also "DHS/USCIS/ICE/CBP-001". Matching only one slash
# silently truncates the business key to "ICE/CBP-001".
SYSTEM_NUMBER_RE = re.compile(r"\b([A-Z]{2,6}(?:/[A-Z]{2,10}){0,4})\s*[-–]{1,2}\s*(\d{3})\b")
SOURCE_URL_RE = re.compile(r"Source URL:\s*(\S+)")
DOCKET_RE = re.compile(r"\[Docket No\.\s*([^\]]+)\]")
FR_DOC_NO_RE = re.compile(r"\[FR Doc No:\s*([^\]]+)\]")
FR_VOLUME_RE = re.compile(
    r"\[Federal Register Volume\s+(\d+),\s*Number\s+(\d+)\s*\(([^)]+)\)\]"
)
FR_PAGES_RE = re.compile(r"\[Pages?\s+([0-9\-]+)\]")
FR_CITATION_RE = re.compile(r"\b(\d{2,3})\s+FR\s+(\d{3,6})\b")
FILENAME_ID_RE = re.compile(r"ID[_-](.+?)(?:\.docx)?$", re.IGNORECASE)

DATE_FORMATS = ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y")
DATE_TEXT_RE = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+\d{1,2},\s+\d{4}\b"
)
URL_DATE_RE = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")


def normalize_date(value: str | None) -> str | None:
    """Parse a human-written date into ISO yyyy-mm-dd, or None."""
    if not value:
        return None
    value = value.strip().rstrip(".")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def classify_action(action_text: str) -> str:
    """Bucket the ACTION line into a small controlled vocabulary."""
    lowered = (action_text or "").lower()
    if "rescind" in lowered:
        return "rescindment"
    if "modif" in lowered or "amend" in lowered:
        return "modified"
    if "updat" in lowered or "reissu" in lowered:
        return "updated"
    if "new" in lowered:
        return "new"
    if "notice" in lowered:
        return "notice"
    return "unknown"


def _first_date(text: str) -> str | None:
    match = DATE_TEXT_RE.search(text or "")
    return normalize_date(match.group(0)) if match else None


def extract_metadata(
    *,
    path: Path,
    lines: list[str],
    text: str,
    sections: dict[str, dict],
    front_matter: dict[str, str],
    pages: list[str],
    hard_wrapped: bool,
) -> dict:
    """Pull typed, filterable metadata out of the notice."""
    title_line = lines[0].strip() if lines else path.stem
    source_url_match = SOURCE_URL_RE.search("\n".join(lines[:5]))
    source_url = source_url_match.group(1) if source_url_match else None

    # Prefer the system number from the title line; it is the most reliable
    # place it appears and is not affected by body reflow.
    system_number = None
    component = None
    agency = None
    number_match = SYSTEM_NUMBER_RE.search(title_line) or SYSTEM_NUMBER_RE.search(text[:4000])
    if number_match:
        prefix, digits = number_match.group(1), number_match.group(2)
        system_number = f"{prefix}-{digits}"
        if "/" in prefix:
            # Owning department first, the owning component(s) after. Kept as a
            # single string for joint systems ("USCIS/ICE/CBP").
            agency, component = prefix.split("/", 1)
        else:
            agency = prefix

    fr_volume = fr_number = fr_pub_date = None
    volume_match = FR_VOLUME_RE.search(text)
    if volume_match:
        fr_volume = int(volume_match.group(1))
        fr_number = int(volume_match.group(2))
        # "Tuesday, December 16, 2014" -> strip the weekday before parsing.
        fr_pub_date = _first_date(volume_match.group(3))

    if not fr_pub_date and source_url:
        url_date = URL_DATE_RE.search(source_url)
        if url_date:
            fr_pub_date = "-".join(url_date.groups())

    fr_doc_match = FR_DOC_NO_RE.search(text)
    fr_doc_number = fr_doc_match.group(1).strip() if fr_doc_match else None
    if not fr_doc_number:
        name_match = FILENAME_ID_RE.search(path.name)
        if name_match:
            fr_doc_number = name_match.group(1).strip()

    pages_match = FR_PAGES_RE.search(text)
    docket_match = DOCKET_RE.search(text)

    citations = sorted({f"{v} FR {p}" for v, p in FR_CITATION_RE.findall(text)})

    action_text = front_matter.get("fm_action", "")
    dates_text = front_matter.get("fm_dates", "")

    return {
        "title": title_line,
        "system_number": system_number,
        "agency": agency,
        "component": component,
        "source_url": source_url,
        "docket_number": docket_match.group(1).strip() if docket_match else None,
        "fr_doc_number": fr_doc_number,
        "fr_volume": fr_volume,
        "fr_number": fr_number,
        "fr_pages": pages_match.group(1) if pages_match else None,
        "fr_publication_date": fr_pub_date,
        "effective_date": _first_date(dates_text) or fr_pub_date,
        "action_raw": action_text or None,
        "action_type": classify_action(action_text),
        "agency_line": front_matter.get("fm_agency") or None,
        "summary": front_matter.get("fm_summary") or None,
        "dates_raw": dates_text or None,
        "contact_raw": front_matter.get("fm_contact") or None,
        "security_classification": (
            sections.get("security_classification", {}).get("text") or None
        ),
        "fr_citations": citations,
        "page_markers": pages,
        "format_variant": "pre_2016" if hard_wrapped else "omb_a108_2016",
    }


# ---------------------------------------------------------------------------
# 7. TOP-LEVEL PARSE
# ---------------------------------------------------------------------------
# Sections a usable SORN should have. Absence is reported rather than raised,
# because a partially-captured notice is still worth landing in bronze with a
# flag on it.
EXPECTED_SECTIONS = [
    "system_name_and_number",
    "system_location",
    "categories_of_individuals",
    "categories_of_records",
    "authority_for_maintenance",
    "purpose",
    "routine_uses",
    "record_access_procedures",
]


def parse_sorn(docx_path: str | Path) -> dict:
    """Parse one SORN .docx into a structured record.

    Never raises on thin or malformed input. A corpus of scraped government
    documents always contains a few bad captures, and a parser that throws on
    them turns a batch load into a game of whack-a-mole. Instead every record
    comes back with a ``parse_status`` and ``quality_flags`` describing what was
    and was not recoverable, so the loader can decide what to skip and you can
    filter on it in SQL later.

    The four statuses map to things actually found in this corpus:

        ok          all expected sections present (21 of 26 documents)
        partial     parsed fine, but the source genuinely omits a section.
                    DHS/CBP-011 has no "Purpose" heading and DHS/ICE-013 has no
                    "System name" heading -- verified by grepping the text, not
                    assumed. These are real notices and worth loading.
        empty       the capture returned only a title and source URL.
                    DHS/CBP-023 and DHS/USCG-031 are both regulations.gov
                    scrapes that came back with no body at all.
        not_a_sorn  substantial text but zero SORN sections, because it is a
                    different kind of Privacy Act document. DHS/USVISIT-004 is
                    a Final Rule on exemptions, not a system of records notice.

    The distinction between the last two matters: "empty" is a broken download
    worth re-fetching, while "not_a_sorn" is a perfectly good document that
    simply does not belong in a SORN corpus. Collapsing them into one failure
    bucket would send you chasing a scrape that was never wrong.
    """
    path = Path(docx_path)
    lines = extract_docx_lines(path)
    hard_wrapped = looks_hard_wrapped(lines)
    reflowed = reflow(lines, hard_wrapped)
    clean_text, pages = normalize_text(reflowed)

    sections, front_matter = split_sections(clean_text)
    routine_uses = split_routine_uses(sections.get("routine_uses", {}).get("text", ""))

    metadata = extract_metadata(
        path=path,
        lines=lines,
        text=clean_text,
        sections=sections,
        front_matter=front_matter,
        pages=pages,
        hard_wrapped=hard_wrapped,
    )

    missing = [key for key in EXPECTED_SECTIONS if key not in sections]

    # An "empty" capture is one where nothing but the title/source-URL header
    # survived. 031 in this corpus is exactly that: the regulations.gov scrape
    # returned no body. It must not look like a successful ingest.
    body_chars = len(clean_text)
    is_empty = len(sections) == 0 and body_chars < 500

    # A notice with real text but no SORN sections at all is a different kind
    # of Privacy Act document (the corpus contains a Final Rule on exemptions).
    # It is not a damaged SORN, so it gets its own status rather than being
    # reported as a parse failure.
    not_a_sorn = len(sections) == 0 and not is_empty

    if is_empty:
        parse_status = "empty"
    elif not_a_sorn:
        parse_status = "not_a_sorn"
    elif missing:
        parse_status = "partial"
    else:
        parse_status = "ok"

    quality_flags = []
    if is_empty:
        quality_flags.append("no_body_text")
    if not_a_sorn:
        quality_flags.append("no_sorn_sections")
    if missing and not not_a_sorn:
        quality_flags.append("missing_sections")
    if not routine_uses and not is_empty and not not_a_sorn:
        quality_flags.append("no_routine_uses_parsed")
    if not metadata["system_number"]:
        quality_flags.append("no_system_number")

    # Business key, and the MERGE key the loader upserts on. The system number
    # is the right choice because it is the government's own stable identifier
    # for the system: when DHS reissues DHS/USCG-029 in 2027, that revision
    # should replace the 2017 row rather than sit beside it. Keying on the
    # filename or the FR document number would accumulate a new row per
    # revision instead.
    #
    # Falls back to the filename so an unparseable document still gets a
    # stable, non-colliding identity rather than colliding with every other
    # unparseable one on a shared "UNKNOWN".
    source_id = metadata["system_number"] or f"UNKNOWN/{path.stem[:60]}"

    return {
        "doc_type": "sorn",
        "source_id": source_id,
        "source_file": path.name,
        "raw_text": clean_text,
        "content_sha256": hashlib.sha256(clean_text.encode("utf-8")).hexdigest(),
        "char_count": body_chars,
        "line_count": len(lines),
        "parser_version": PARSER_VERSION,
        "parse_status": parse_status,
        "quality_flags": quality_flags,
        "missing_sections": missing,
        "metadata": metadata,
        "sections": [
            {
                "section_key": key,
                "heading": value["heading"],
                "ordinal": value["ordinal"],
                "text": value["text"],
                "char_count": len(value["text"]),
            }
            for key, value in sorted(sections.items(), key=lambda kv: kv[1]["ordinal"])
        ],
        "routine_uses": routine_uses,
    }


def parse_directory(directory: str | Path, pattern: str = "*.docx") -> list[dict]:
    """Parse every .docx in a directory, sorted by filename."""
    return [parse_sorn(p) for p in sorted(Path(directory).glob(pattern))]


if __name__ == "__main__":
    import json
    import sys

    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    records = parse_directory(target) if target.is_dir() else [parse_sorn(target)]

    for record in records:
        print(
            f"{record['source_id']:<20} {record['parse_status']:<8} "
            f"chars={record['char_count']:<7} sections={len(record['sections']):<3} "
            f"routine_uses={len(record['routine_uses']):<3} {record['source_file']}"
        )
        if record["quality_flags"]:
            print(f"    flags: {', '.join(record['quality_flags'])}")

    if len(records) == 1:
        print(json.dumps(records[0]["metadata"], indent=2))
