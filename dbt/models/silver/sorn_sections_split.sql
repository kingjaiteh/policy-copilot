-- Sub-splits ONLY the SORN sections too long to embed intact. Sections that
-- already fit are not selected here and never appear.
--
-- This is arm B of the chunking A/B; see the header of gold/document_chunks.sql
-- for how the two arms are tagged and queried. The splitting algorithm itself
-- lives in macros/chunk_split_parts.sql, shared with nist_controls_split.

{{ config(materialized='table') }}

with parts as (

    {{ chunk_split_parts(
           relation=ref('sorn_sections'),
           keys=['source_id', 'section_key'],
           text_column='section_text',
           where_clause=sorn_section_is_indexable('section_key')
                        ~ ' and '
                        ~ sorn_chunk_is_oversized(
                              'document_title', 'agency', 'heading', 'section_text')
       ) }}

)

select
    p.source_id,
    p.section_key,
    s.heading,
    s.document_title,
    s.agency,
    s.system_number,
    s.effective_date,
    s.source_url,
    s.content_sha256,
    s.section_ordinal,
    s.ingested_at,
    p.part_no,
    p.total_parts,
    p.part_text,
    p.part_char_count
from parts p
-- Join back for the carried metadata rather than threading a dozen columns
-- through the aggregation inside the macro. Same result, and it keeps the
-- macro's grouping keyed on exactly what identifies a section.
join {{ ref('sorn_sections') }} s
    on  p.source_id   = s.source_id
    and p.section_key = s.section_key
