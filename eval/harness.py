"""Retrieval evaluation harness for the chunking A/B.

    python eval/harness.py --backend local
    python eval/harness.py --backend databricks --index policy_copilot.gold.chunks_idx

Scores the same labelled question set against both arms of the experiment and
prints a per-arm comparison. See eval/README.md for what the numbers mean and,
more importantly, for what the local backend cannot tell you.

THE TWO ARMS
    whole  (A)  chunk_strategy != 'sub_split'         over-ceiling sections kept intact
    split  (B)  chunk_strategy != 'oversized_whole'   the same sections as parts

Both arms read the SAME index. Only the filter differs, so embedding model,
corpus, and query are held constant and chunking is the only variable.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

from chunking import Chunk, build_corpus  # noqa: E402

ARMS = ("whole", "split")
DEFAULT_K = 5


# --- Question set -----------------------------------------------------------

@dataclass
class Question:
    id: str
    question: str
    targets: set[tuple[str, str | None]]
    cls: str
    answer_locus: str


def load_questions(path: Path) -> list[Question]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    out = []
    for item in raw["questions"]:
        out.append(Question(
            id=item["id"],
            question=item["question"],
            targets={(t["source_id"], t.get("section_key")) for t in item["targets"]},
            cls=item.get("class", "contested"),
            answer_locus=item.get("answer_locus", "unknown"),
        ))
    return out


# --- Metrics ----------------------------------------------------------------
# Ranked lists are lists of (source_id, section_key) grading keys, in rank
# order. Relevance is binary: a retrieved chunk is relevant if its key is in
# the question's target set.

def hit_at_k(retrieved: list[tuple], targets: set[tuple], k: int) -> float:
    """1.0 if any target appears in the top k. The blunt "did it work" metric."""
    return 1.0 if any(r in targets for r in retrieved[:k]) else 0.0


def recall_at_k(retrieved: list[tuple], targets: set[tuple], k: int) -> float:
    """Fraction of distinct targets found in the top k."""
    if not targets:
        return 0.0
    return len(set(retrieved[:k]) & targets) / len(targets)


def mrr(retrieved: list[tuple], targets: set[tuple], k: int) -> float:
    """Reciprocal rank of the FIRST relevant result.

    More sensitive than hit@k to the thing chunking is supposed to change:
    moving the right passage from rank 4 to rank 1 shows up here and is
    invisible to hit@5.
    """
    for i, key in enumerate(retrieved[:k], start=1):
        if key in targets:
            return 1.0 / i
    return 0.0


def ndcg_at_k(retrieved: list[tuple], targets: set[tuple], k: int) -> float:
    """Binary-gain nDCG@k, deduplicated by grading key.

    NOTE ON A SUBTLETY THAT MATTERS FOR THIS EXPERIMENT: arm B can return
    SEVERAL chunks that map to the same target (three parts of one section).
    Counting each as a separate gain would inflate arm B's DCG for no real
    retrieval benefit -- the user learns nothing from the second copy of the
    same section. So gains are credited once per distinct target, which is
    what `seen` below is for. Without it the A/B is biased toward whichever
    arm produces more chunks, which is arm B by construction.
    """
    gains, seen = [], set()
    for key in retrieved[:k]:
        relevant = key in targets and key not in seen
        gains.append(1.0 if relevant else 0.0)
        if relevant:
            seen.add(key)

    dcg = sum(g / math.log2(i + 2) for i, g in enumerate(gains))
    ideal = min(len(targets), k)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(ideal))
    return dcg / idcg if idcg else 0.0


METRICS = {
    "hit@k": hit_at_k,
    "recall@k": recall_at_k,
    "mrr": mrr,
    "ndcg@k": ndcg_at_k,
}


# --- Backends ---------------------------------------------------------------

class LocalBM25Backend:
    """Offline backend: builds both arms locally and retrieves with BM25.

    READ THIS BEFORE TRUSTING ITS NUMBERS.

    BM25 is LEXICAL. The production index is DENSE. They respond to chunking
    differently in ways that matter here:

      * BM25 has its own length normalisation (the `b` parameter) and no
        context window, so it never truncates. The single largest benefit of
        splitting in production -- that content past the model's window is
        embedded at all instead of being silently dropped -- is invisible to
        BM25, because BM25 was indexing that text either way.
      * Dense retrieval blurs a long passage into one averaged vector, so
        splitting sharpens it. BM25 has no such averaging effect.

    So this backend answers "is the harness wired up correctly, do the arms
    actually differ, and is the chunker producing sane text" -- all of which
    are worth knowing before spending a re-embed. It does NOT answer "should
    we ship arm B." Only the databricks backend answers that.
    """

    K1 = 1.5
    B = 0.75
    TOKEN = re.compile(r"[a-z0-9]+")

    def __init__(self) -> None:
        self.corpus = build_corpus()
        self._index: dict[str, tuple] = {}
        for arm in ARMS:
            self._index[arm] = self._build(self.corpus.arm(arm))

    @classmethod
    def _tokenize(cls, text: str) -> list[str]:
        return cls.TOKEN.findall(text.lower())

    def structural_problems(self) -> list[str]:
        """Assert the two arms differ ONLY in how oversized sections appear.

        Checkable here because this backend holds the whole corpus. The
        Databricks backend cannot do it from query results, and does not need
        to: assert_sub_split_covers_oversized asserts the same thing
        warehouse-side, over all rows rather than the ones a query returned.
        """
        problems: list[str] = []

        whole = {c.chunk_id: c for c in self.corpus.arm("whole")}
        split = {c.chunk_id: c for c in self.corpus.arm("split")}

        standard = {c.chunk_id for c in self.corpus.chunks
                    if c.chunk_strategy == "standard"}

        missing_a = standard - set(whole)
        missing_b = standard - set(split)
        if missing_a:
            problems.append(f"{len(missing_a)} 'standard' chunk(s) absent from arm A")
        if missing_b:
            problems.append(f"{len(missing_b)} 'standard' chunk(s) absent from arm B")

        differing = [cid for cid in (set(whole) & set(split))
                     if whole[cid].chunk_text != split[cid].chunk_text]
        if differing:
            problems.append(
                f"{len(differing)} chunk(s) present in both arms with DIFFERENT text, "
                f"e.g. {differing[0]}")

        leaked_a = [c for c in whole.values() if c.chunk_strategy == "sub_split"]
        leaked_b = [c for c in split.values() if c.chunk_strategy == "oversized_whole"]
        if leaked_a:
            problems.append(f"{len(leaked_a)} 'sub_split' chunk(s) leaked into arm A")
        if leaked_b:
            problems.append(f"{len(leaked_b)} 'oversized_whole' chunk(s) leaked into arm B")

        return problems

    def _build(self, chunks: list[Chunk]):
        docs = [self._tokenize(c.chunk_text) for c in chunks]
        lengths = [len(d) for d in docs]
        avg_len = sum(lengths) / len(lengths) if lengths else 0.0

        postings: dict[str, list[tuple[int, int]]] = defaultdict(list)
        for doc_id, tokens in enumerate(docs):
            for term, freq in Counter(tokens).items():
                postings[term].append((doc_id, freq))

        n_docs = len(docs)
        idf = {
            term: math.log(1 + (n_docs - len(plist) + 0.5) / (len(plist) + 0.5))
            for term, plist in postings.items()
        }
        return chunks, lengths, avg_len, postings, idf

    def search(self, query: str, arm: str, k: int) -> list[Chunk]:
        chunks, lengths, avg_len, postings, idf = self._index[arm]
        scores: dict[int, float] = defaultdict(float)

        for term in self._tokenize(query):
            if term not in postings:
                continue
            term_idf = idf[term]
            for doc_id, freq in postings[term]:
                norm = 1 - self.B + self.B * (lengths[doc_id] / avg_len) if avg_len else 1
                scores[doc_id] += term_idf * (freq * (self.K1 + 1)) / (freq + self.K1 * norm)

        ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[:k]
        return [chunks[doc_id] for doc_id, _ in ranked]


class DatabricksIndexBackend:
    """Queries the real Delta Sync vector index, one arm at a time.

    Both arms hit the same index; `filters_json` selects the arm. That string
    is the documented shape for the Vector Search query API -- a JSON object of
    column-predicate pairs, where a bare 'col NOT' key means "not equal to".
    It is NOT a SQL fragment: passing 'chunk_strategy != "sub_split"' silently
    fails to filter rather than erroring, which would quietly collapse the A/B
    into two identical arms.
    """

    def __init__(self, index_name: str) -> None:
        from databricks.sdk import WorkspaceClient

        self.index_name = index_name
        self.client = WorkspaceClient()

    ARM_FILTERS = {
        "whole": {"chunk_strategy NOT": ["sub_split"]},
        "split": {"chunk_strategy NOT": ["oversized_whole"]},
    }

    def search(self, query: str, arm: str, k: int) -> list[Chunk]:
        response = self.client.vector_search_indexes.query_index(
            index_name=self.index_name,
            columns=["chunk_id", "doc_type", "chunk_type", "chunk_strategy",
                     "source_id", "section_key", "chunk_text"],
            query_text=query,
            num_results=k,
            filters_json=json.dumps(self.ARM_FILTERS[arm]),
        )

        # The API returns positional rows plus a column manifest, not dicts.
        manifest = [c.name for c in (response.manifest.columns or [])]
        rows = (response.result.data_array or []) if response.result else []

        out = []
        for row in rows:
            record = dict(zip(manifest, row))
            out.append(Chunk(
                chunk_id=record.get("chunk_id", ""),
                doc_type=record.get("doc_type", ""),
                chunk_type=record.get("chunk_type", ""),
                chunk_strategy=record.get("chunk_strategy", ""),
                source_id=record.get("source_id", ""),
                section_key=record.get("section_key"),
                chunk_text=record.get("chunk_text", ""),
            ))
        return out


# --- Evaluation -------------------------------------------------------------

def evaluate(backend, questions: list[Question], k: int) -> dict:
    per_question: dict[str, dict] = {}

    for question in questions:
        row = {"question": question, "arms": {}}
        for arm in ARMS:
            results = backend.search(question.question, arm, k)
            retrieved = [c.target for c in results]
            row["arms"][arm] = {
                "retrieved": retrieved,
                "chunk_ids": [c.chunk_id for c in results],
                "scores": {
                    name: fn(retrieved, question.targets, k)
                    for name, fn in METRICS.items()
                },
            }
        per_question[question.id] = row

    return per_question


def aggregate(per_question: dict, cls: str | None = None) -> dict[str, dict[str, float]]:
    rows = [r for r in per_question.values()
            if cls is None or r["question"].cls == cls]
    if not rows:
        return {}

    out: dict[str, dict[str, float]] = {}
    for arm in ARMS:
        out[arm] = {
            name: sum(r["arms"][arm]["scores"][name] for r in rows) / len(rows)
            for name in METRICS
        }
    out["_n"] = len(rows)
    return out


def report(per_question: dict, k: int, verbose: bool, backend) -> int:
    label = {"hit@k": f"hit@{k}", "recall@k": f"recall@{k}",
             "mrr": f"MRR@{k}", "ndcg@k": f"nDCG@{k}"}

    print()
    print("=" * 74)
    print(f"CHUNKING A/B  --  arm A = 'whole' (unsplit), arm B = 'split'   k={k}")
    print("=" * 74)

    for cls, title in (("contested", "CONTESTED  (answer sits in an over-ceiling section)"),
                       ("control", "CONTROL    (answer identical in both arms)"),
                       (None, "ALL QUESTIONS")):
        agg = aggregate(per_question, cls)
        if not agg:
            continue
        n = agg.pop("_n")
        print(f"\n{title}   n={n}")
        print(f"  {'metric':<12} {'A whole':>9} {'B split':>9} {'delta':>9}")
        for name in METRICS:
            a, b = agg["whole"][name], agg["split"][name]
            arrow = "  " if abs(b - a) < 1e-9 else (" +" if b > a else " -")
            print(f"  {label[name]:<12} {a:>9.3f} {b:>9.3f} {arrow}{abs(b - a):>7.3f}")

    # STRUCTURAL VALIDITY. This is the hard check, and it deliberately tests
    # the CORPUS rather than the scores.
    #
    # An earlier version asserted that control questions must score identically
    # in both arms, on the reasoning that they only touch chunks present in
    # both. That reasoning is wrong, and the check fired on a healthy pipeline:
    # arm B holds 72 more chunks than arm A, and BM25's IDF and average
    # document length are corpus-wide statistics, so EVERY score shifts a
    # little in arm B whether or not the question touches split content. The
    # target chunk for the question that tripped it was byte-identical and
    # present in both arms; it simply moved from rank 1 to rank 2.
    #
    # What actually needs to hold is structural, so that is what is asserted:
    # the arms must contain the same 'standard' chunks, byte for byte, and each
    # must exclude the other's variant rows. A failure here is a tagging or
    # filter bug and does invalidate everything above it.
    print("\n" + "-" * 74)
    problems = getattr(backend, "structural_problems", lambda: None)()

    if problems is None:
        print("structural validity: not checkable from query results alone.")
        print("  The dbt tests assert it warehouse-side -- assert_sub_split_covers_oversized")
        print("  for arm coverage, and the accepted_values test on chunk_strategy.")
    elif problems:
        print(f"STRUCTURAL FAILURE: {len(problems)} problem(s). The arms differ by")
        print("  something other than chunking, so the numbers above are not")
        print("  trustworthy.")
        for problem in problems:
            print(f"    - {problem}")
    else:
        print("structural validity ok: both arms carry the same 'standard' chunks "
              "byte for byte,")
        print("  and neither carries the other's variant rows.")

    # CROWDING. Arm B has more chunks, so they compete for the same top-k
    # slots. Reported as a measurement, not a failure: in a dense index this is
    # a real cost of splitting. On the local backend it is inflated by the BM25
    # statistics drift described above, so read it as an upper bound.
    drifted = [qid for qid, row in per_question.items()
               if row["question"].cls == "control"
               and row["arms"]["whole"]["scores"] != row["arms"]["split"]["scores"]]
    n_control = sum(1 for r in per_question.values() if r["question"].cls == "control")
    print()
    if drifted:
        print(f"crowding: {len(drifted)}/{n_control} control question(s) changed rank "
              f"in arm B ({', '.join(drifted)}).")
        print("  Their target chunks are identical in both arms, so this is arm B's "
              "extra")
        print("  chunks competing for top-k slots -- a genuine cost of splitting, and "
              "the")
        print("  reason the contested gain should be read net of it.")
    else:
        print(f"crowding: none. All {n_control} control questions held their ranking "
              f"in arm B.")

    if verbose:
        print("\n" + "=" * 74)
        print("PER-QUESTION DETAIL (contested questions where the arms disagree)")
        print("=" * 74)
        for qid, row in per_question.items():
            q = row["question"]
            if q.cls != "contested":
                continue
            a = row["arms"]["whole"]["scores"]["mrr"]
            b = row["arms"]["split"]["scores"]["mrr"]
            if abs(a - b) < 1e-9:
                continue
            verdict = "split wins" if b > a else "whole wins"
            print(f"\n{qid}  [{q.answer_locus}]  MRR {a:.3f} -> {b:.3f}   {verdict}")
            print(f"  {q.question}")
            print(f"  want: {sorted(q.targets)}")
            # Full top-k, not a preview: truncating hides the case where the
            # arms return the same leading results and differ only further
            # down, which is exactly what a change in MRR is reporting.
            for arm in ARMS:
                for rank, chunk_id in enumerate(row["arms"][arm]["chunk_ids"], start=1):
                    marker = "*" if row["arms"][arm]["retrieved"][rank - 1] in q.targets else " "
                    print(f"  {arm if rank == 1 else '':<6} {marker}{rank}. {chunk_id}")

    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backend", choices=("local", "databricks"), default="local",
                        help="local: offline BM25 over locally built chunks. "
                             "databricks: the real vector index.")
    parser.add_argument("--index", help="index name, required for --backend databricks")
    parser.add_argument("-k", type=int, default=DEFAULT_K, help="retrieval depth")
    parser.add_argument("--questions", type=Path,
                        default=Path(__file__).resolve().parent / "questions.yml")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show per-question detail where the arms disagree")
    args = parser.parse_args()

    questions = load_questions(args.questions)

    if args.backend == "local":
        print("backend: local BM25 (lexical). Validates the harness and the "
              "chunker, NOT\n         a substitute for measuring the real "
              "embedding index -- see eval/README.md.")
        backend = LocalBM25Backend()
    else:
        if not args.index:
            parser.error("--index is required with --backend databricks")
        backend = DatabricksIndexBackend(args.index)

    per_question = evaluate(backend, questions, args.k)
    return report(per_question, args.k, args.verbose, backend)


if __name__ == "__main__":
    raise SystemExit(main())
