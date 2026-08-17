# Retrieval evaluation

A labelled question set and a scorer for the chunking A/B, plus an offline
mirror of the chunker so both can be exercised without a warehouse.

```
eval/
├── questions.yml   33 labelled questions: 21 contested, 12 control
├── chunking.py     Python mirror of the dbt chunking models + a self-check
└── harness.py      scores both arms, prints the comparison
```

## The experiment

Free Edition gives one vector search endpoint and one search unit, so we cannot
stand up two indexes and compare them. Both chunking strategies therefore live
in **one** index, tagged with `chunk_strategy`, and the arms are selected by
filtering at query time:

| arm | filter | over-ceiling SORN sections are… |
|-----|--------|-------------------------------|
| A `whole` | `chunk_strategy != 'sub_split'` | kept intact |
| B `split` | `chunk_strategy != 'oversized_whole'` | replaced by overlapping parts |

Chunks tagged `standard` — everything that already fits, all routine uses, all
NIST controls — appear in **both** arms, stored once. Same index, same
embedding model, same questions; chunking is the only variable.

## Running it

The `local` backend rebuilds the corpus from source, so it needs two things the
repo deliberately does not carry: `data/sorn_docx/*.docx` and
`scripts/nist_payloads.json` (regenerate the latter with
`python scripts/fetch_nist_controls.py`). The `databricks` backend needs
neither — it reads the index.

```powershell
# offline: builds both arms from data/sorn_docx and retrieves with BM25
python eval/harness.py --backend local -v

# against the real index
databricks auth login --profile dbc-1d037021-8869
python eval/harness.py --backend databricks --index policy_copilot.gold.<index_name>

# what the chunker does to this corpus, and whether it holds its invariants
python eval/chunking.py
```

## What the local backend can and cannot tell you

**It is not a substitute for measuring the real index.** BM25 is lexical, the
production index is dense, and they respond to chunk size differently in two
ways that both cut against reading the offline number as a prediction:

- **BM25 never truncates.** The single largest benefit of splitting in
  production is that text past the embedding model's context window gets
  embedded at all instead of being silently dropped. BM25 was indexing that
  text either way, so the effect is invisible here.
- **BM25's length normalisation independently favours short documents.** The
  `b` parameter shrinks scores for longer documents, so arm B gets a tailwind
  that has nothing to do with retrieval quality. Some of the offline margin is
  that artefact, and there is no clean way to separate it out.

What the local backend *is* good for: proving the harness is wired up, that the
arms genuinely differ, that ground truth matches real chunks, and that the
chunker produces sane text — all before spending a full refresh and a re-embed
to find out.

## Ground truth is `(source_id, section_key)`, not `chunk_id`

The most important design decision in `questions.yml`. `chunk_id` differs
between arms by construction: arm A returns
`sorn:DHS/CBP-024:sec:policies_retention`, arm B returns the same content as
`:p0`, `:p1`, `:p2`. Labelling ground truth with chunk_ids would score arm B as
a total miss on every contested question and hand arm A a landslide that is
pure artefact.

`(source_id, section_key)` is stable across arms, which is why
`gold.document_chunks` carries `section_key` as a real column rather than
leaving it to be regexed back out of the key. For NIST, `section_key` is null
and `source_id` is the control id.

## Contested vs control questions

**Contested** (21): the answer lives in a section that is over the embedding
ceiling, so the arms genuinely differ. These are the only questions that can
move the numbers.

**Control** (12): the answer lives in a chunk that is byte-identical in both
arms — a routine use, a short section, a NIST control.

Control questions are **not** guaranteed to score identically, and an earlier
version of this harness was wrong to assert that they were. Arm B holds 72 more
chunks than arm A. Those extra chunks compete for the same top-k slots, and on
the local backend they also shift BM25's IDF and average-document-length, which
are corpus-wide statistics — so every score moves a little in arm B whether or
not the question touches split content. The check fired on a perfectly healthy
pipeline for exactly that reason: `c12`'s target chunk was byte-identical and
present in both arms, and merely moved from rank 1 to rank 2.

So the harness separates two things:

- **Structural validity** (hard, fails the run): both arms carry the same
  `standard` chunks byte for byte, neither carries the other's variant rows.
  A failure here is a tagging or filter bug and does invalidate everything
  above it. Checked over the whole corpus by the local backend; asserted
  warehouse-side by `assert_sub_split_covers_oversized` for the real index.
- **Crowding** (reported, not failed): how many control questions changed rank
  in arm B. In a dense index this is a *real* cost of splitting — more chunks
  competing for the same slots — so the contested gain should be read net of
  it. On the local backend it is inflated by the BM25 statistics drift above,
  so treat it as an upper bound.

`answer_locus` records where in the section the answer sits (`head`, `middle`,
`tail`). Splitting should help most for `tail`: content far enough into a long
section that a single embedding dilutes it, and that truncation drops outright.

## Metrics

`hit@k`, `recall@k`, `MRR@k`, `nDCG@k`, all with binary relevance.

MRR is the most sensitive to what chunking actually changes — moving the right
passage from rank 4 to rank 1 shows up there and is invisible to `hit@5`.

One deliberate choice in `ndcg_at_k`: arm B can return several chunks mapping to
the same target (three parts of one section). Gains are credited **once per
distinct target**, because the reader learns nothing from the second copy of the
same section. Without that dedup, nDCG is biased toward whichever arm emits more
chunks, which is arm B by construction.

## Current offline result

`python eval/harness.py --backend local -k 5`, 33 questions:

| | contested (n=21) A → B | control (n=12) A → B |
|---|---|---|
| hit@5 | 0.810 → 0.952 | 0.917 → 0.917 |
| MRR@5 | 0.583 → 0.917 | 0.778 → 0.736 |
| nDCG@5 | 0.639 → 0.925 | 0.813 → 0.783 |

Structural validity passes; crowding shows 1 of 12 control questions losing a
rank in arm B.

Directionally consistent with the hypothesis, and the crowding cost is small
relative to the contested gain. Read the size of the margin with the two BM25
caveats above firmly in mind — **this is not yet evidence for shipping arm B.**

## Keeping the mirror honest

`chunking.py` reimplements the dbt models in Python and can drift from them. The
SQL is the source of truth. `python eval/chunking.py` prints the statistics the
SQL should reproduce and asserts the same invariants the dbt tests do, so the
two can be compared deliberately:

```
chunks by strategy (should match gold.document_chunks):
  standard          1225
  oversized_whole     37
  sub_split          109
  TOTAL             1371
```

Verified against the warehouse on 2026-08-16: identical, including the longest
non-control chunk at 1,990 characters. Two bugs were caught by that comparison
rather than by the mirror, which is the point of doing it — the mirror applied
gold's section exclusions before splitting while the SQL applied them only in
gold (275 `sub_split` rows instead of 109), and the mirror read the parser's raw
routine-use text while gold embeds `routine_use_full_text` with subitems folded
in (three chunks over the ceiling).

Cross-check against the warehouse with:

```sql
SELECT chunk_strategy, count(*)
FROM policy_copilot.gold.document_chunks
GROUP BY chunk_strategy ORDER BY 1;
```

If those disagree, the SQL is right and the mirror needs updating.
