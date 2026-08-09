-- SINGULAR TEST (CRITICAL)
--
-- ROW-COUNT PARITY: there must be at least as many section rows as there are
-- SORN documents. If the explode in sorn_sections ever starts dropping
-- documents wholesale, the count collapses and this catches it.
--
-- WHY THIS IS NOT A GENERIC TEST: dbt_utils.fewer_rows_than asserts a strict
-- `<` in one direction only, so it cannot express `>=` parity. Pointing it at
-- stg_sorn asserted that sections were FEWER than documents, which is backwards
-- and failed on every healthy run. A one-SORN-one-section corpus is also
-- legitimate, so a strict inequality in either direction is wrong here.
--
-- Distinct from assert_no_documents_lost: that one catches an INDIVIDUAL
-- document reaching bronze and producing no chunks. This catches the aggregate
-- collapse, including the case where the explode returns nothing at all.

with counts as (

    select
        (select count(*) from {{ ref('sorn_sections') }}) as section_rows,
        (select count(*) from {{ ref('stg_sorn') }})      as document_rows

)

select
    section_rows,
    document_rows
from counts
where section_rows < document_rows
