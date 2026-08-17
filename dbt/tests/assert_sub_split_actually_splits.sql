-- SINGULAR TEST (CRITICAL)
--
-- Every oversized section must yield at least TWO sub_split parts.
--
-- WHY THIS TEST EXISTS, SPECIFICALLY
--
--   The first version of the splitter divided sections on '\n\n'. The parser
--   never emits a blank line, so split() returned a single-element array and
--   every "part" was a byte-for-byte copy of the whole section. It produced
--   rows. It produced correctly-formed chunk_ids. It satisfied the coverage
--   test in assert_sub_split_covers_oversized.sql, because one part per
--   section is still coverage. And it had done nothing whatsoever.
--
--   That is the dangerous class of bug in an experiment: it does not fail, it
--   produces a NUMBER. Arm B would have scored identically to arm A, and the
--   readout "splitting makes no difference" is indistinguishable from the
--   truth, which was "we never split anything." You would then have concluded
--   something false about chunking and moved on.
--
--   A coverage test cannot catch this. Only a test asserting the split had an
--   EFFECT can. Hence: total_parts >= 2, as an error.
--
--   Corollary for whoever changes the separator, the threshold, or the target
--   size later: if this starts failing, the splitter has stopped splitting.
--   Do not weaken it to >= 1.

-- Covers both splitters, because they share the algorithm via
-- macros/chunk_split_parts.sql and would therefore share the bug.

select 'sorn' as corpus, source_id, section_key as unit, total_parts
from {{ ref('sorn_sections_split') }}
group by source_id, section_key, total_parts
having total_parts < 2

union all

select 'nist' as corpus, source_id, cast(null as string) as unit, total_parts
from {{ ref('nist_controls_split') }}
group by source_id, total_parts
having total_parts < 2
