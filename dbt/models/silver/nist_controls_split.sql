-- Splits the NIST controls too long to embed intact.
--
-- WHY THIS MODEL EXISTS, WHEN THE ORIGINAL COMMENT SAID IT DID NOT NEED TO
--
--   gold/document_chunks.sql used to assert that NIST controls "average well
--   under 1,000 characters and top out around 4,800, so none are oversized."
--   That was true against the old 6,000 character threshold. It is false
--   against the real one.
--
--   Sized correctly for bge-large (512 tokens, ~2,000 characters), 27 of the
--   443 controls are over the ceiling -- AC-16 at 4,773 characters, AC-2 at
--   4,380, AU-2 at 3,806. Left whole they do not error, they TRUNCATE, so the
--   tail of AC-2's account management requirements would simply never be
--   embedded while the row still looked complete in the table.
--
--   The original reasoning against splitting controls -- that it separates a
--   requirement from its discussion -- holds for controls that FIT, and those
--   are still emitted whole. For the 27 that do not, the choice is not
--   "split or keep together," it is "split or lose the tail."
--
-- NOT PART OF THE A/B. These parts are tagged 'standard' in gold, so they are
-- present and identical in BOTH arms. The experiment varies exactly one thing,
-- SORN section handling; splitting NIST is a correctness fix that applies
-- either way. If NIST were split in one arm only it would become a second
-- uncontrolled variable and the comparison would stop meaning anything.

{{ config(materialized='table') }}

with parts as (

    {{ chunk_split_parts(
           relation=ref('stg_nist_control'),
           keys=['source_id'],
           text_column='raw_text',
           where_clause="not parse_failed and length(raw_text) > "
                        ~ var('embed_max_chars')
       ) }}

)

select
    p.source_id,
    c.title,
    c.family,
    c.baselines,
    c.is_enhancement,
    c.source_url,
    c.ingested_at,
    p.part_no,
    p.total_parts,
    p.part_text,
    p.part_char_count
from parts p
join {{ ref('stg_nist_control') }} c
    on p.source_id = c.source_id
