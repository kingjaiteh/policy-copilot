-- One row per SORN section. This is the chunking step for SORNs.
--
-- Section boundaries are the right chunk boundary here because a SORN's
-- sections ARE its semantic units: "Categories of Records," "Authority for
-- Maintenance," "Retention and Disposal." Splitting on a fixed character
-- count would cut across those and produce chunks that answer nothing
-- cleanly. This is the main argument for parsing structure before chunking
-- rather than treating the document as a wall of text.

select
    s.source_id,
    s.title           as document_title,
    s.agency,
    s.component,
    s.system_number,
    s.effective_date,
    s.source_url,
    s.content_sha256,

    sec.section_key,
    sec.heading,
    sec.ordinal       as section_ordinal,
    sec.text          as section_text,
    sec.char_count    as section_char_count,

    s.ingested_at

from {{ ref('stg_sorn') }} as s
-- LATERAL VIEW EXPLODE is Spark's way of turning one row with an array of N
-- structs into N rows. `explode` drops rows with empty/null arrays, which is
-- what we want: a SORN that failed to parse contributes no chunks rather than
-- one garbage chunk.
lateral view explode(s.sections) as sec

where not s.parse_failed
  and sec.text is not null
  and length(trim(sec.text)) > 0
