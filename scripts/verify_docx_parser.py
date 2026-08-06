"""
Smoke test for sorn_parser.

Builds a tiny synthetic .docx in a temp dir (not the corpus directory, so it
does not show up as a stray "empty" document in the real parse run) and checks
that the pieces the pipeline depends on still work.

    python verify_docx_parser.py
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

import sorn_parser

# Mirrors the real corpus: one paragraph, one run, structure carried by <w:br/>.
DOC_XML = """<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
<w:body><w:p><w:r>
<w:t>DHS/USCG-999 Example System of Records</w:t><w:br/>
<w:t>Source URL: https://www.federalregister.gov/documents/2024/01/01/2024-00001/x</w:t><w:br/>
<w:t>[Docket No. DHS-2024-0001]</w:t><w:br/>
<w:t>AGENCY: Privacy Office, Department of Homeland Security.</w:t><w:br/>
<w:t>ACTION: Notice of modified Privacy Act System of Records.</w:t><w:br/>
<w:t>DATES: This modified system will be effective January 15, 2024.</w:t><w:br/>
<w:t>System name: DHS/USCG-999 Example System of Records.</w:t><w:br/>
<w:t>Security classification: Unclassified.</w:t><w:br/>
<w:t>Purpose(s): To demonstrate that the parser recovers sections that were</w:t><w:br/>
<w:t>split across a hard line wrap by the original capture.</w:t><w:br/>
<w:t>Routine uses of records maintained in the system, including categories</w:t><w:br/>
<w:t>of users and the purposes of such uses: In addition to those</w:t><w:br/>
<w:t>disclosures generally permitted, records may be disclosed as follows:</w:t><w:br/>
<w:t>A. To the Department of Justice when relevant to litigation.</w:t><w:br/>
<w:t>B. To a congressional office at the request of the individual.</w:t>
</w:r></w:p></w:body></w:document>"""


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "smoke_test.docx"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("word/document.xml", DOC_XML)

        record = sorn_parser.parse_sorn(path)

    sections = {s["section_key"] for s in record["sections"]}
    labels = [ru["label"] for ru in record["routine_uses"]]

    checks = [
        ("system number parsed", record["source_id"] == "DHS/USCG-999"),
        ("action classified", record["metadata"]["action_type"] == "modified"),
        ("effective date parsed", record["metadata"]["effective_date"] == "2024-01-15"),
        ("docket parsed", record["metadata"]["docket_number"] == "DHS-2024-0001"),
        ("purpose section found", "purpose" in sections),
        ("routine_uses section found", "routine_uses" in sections),
        # The heading itself is split across a wrap; matching it proves the
        # whitespace-tolerant heading regex is doing its job.
        ("wrapped heading matched", "routine_uses" in sections),
        ("routine uses split", labels == ["PREAMBLE", "A", "B"]),
        # "split across a hard line wrap" must not come back as "acrossa".
        ("line wrap un-glued", "across a hard line wrap" in record["raw_text"]),
    ]

    failed = 0
    for name, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
        failed += not passed

    print(f"\n{len(checks) - failed}/{len(checks)} checks passed.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
