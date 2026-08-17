-- Splits the handful of routine uses too long to embed intact.
--
-- WHY THIS EXISTS, WHEN ROUTINE USES ARE ALREADY THE ITEM-LEVEL UNIT
--
--   gold/document_chunks.sql claimed routine uses were "all comfortably inside
--   the embedding window," on the basis that the parser's routine-use items
--   run to a median of 375 characters and a maximum of 1,391.
--
--   That measured the wrong column. sorn_routine_uses FOLDS SUBITEMS into
--   routine_use_full_text -- the lettered sub-clauses under a routine use --
--   and that is what gold embeds. Three items cross the ceiling once folded:
--   DHS/USCG-029 (E) at 2,291 characters, DHS/FEMA-002 (E) at 2,287, and
--   DHS/ICE-013 (E) at 2,252. Left whole they truncate silently.
--
--   Three rows out of 347 is a rounding error until you notice that routine
--   uses are the single most-asked-about part of a SORN, and that the tail of
--   a disclosure authority is where the conditions and limits live.
--
-- NOT PART OF THE A/B, for the same reason as nist_controls_split: these parts
-- are tagged 'standard' in gold and appear identically in both arms. Splitting
-- to avoid truncation is a correctness fix that applies either way.

{{ config(materialized='table') }}

with parts as (

    {{ chunk_split_parts(
           relation=ref('sorn_routine_uses'),
           keys=['source_id', 'routine_use_label'],
           text_column='routine_use_full_text',
           where_clause=sorn_chunk_is_oversized(
               'document_title',
               'agency',
               "concat('Routine Use ', routine_use_label)",
               'routine_use_full_text')
       ) }}

)

select
    p.source_id,
    p.routine_use_label,
    r.document_title,
    r.agency,
    r.system_number,
    r.effective_date,
    r.source_url,
    r.content_sha256,
    r.ingested_at,
    p.part_no,
    p.total_parts,
    p.part_text,
    p.part_char_count
from parts p
join {{ ref('sorn_routine_uses') }} r
    on  p.source_id         = r.source_id
    and p.routine_use_label = r.routine_use_label
