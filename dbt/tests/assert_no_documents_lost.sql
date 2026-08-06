-- SINGULAR TEST (CRITICAL)
--
-- The difference between this and the tests in _models.yml: those are
-- "generic" tests, reusable macros applied to a column. A singular test is
-- just a SELECT that should return zero rows. Anything it returns is a failure.
--
-- What this catches: a document that made it into bronze but produced no
-- chunks in gold. That is the worst failure mode in this pipeline, because it
-- is completely silent. The row is in the table, the run is green, and the
-- copilot simply cannot see the document. Nothing else here would catch it.
--
-- This is the same class of gap as the "statement succeeded but nothing
-- landed" bug in the ingestion API: success at every individual step, and the
-- end-to-end outcome still wrong.

with bronze_docs as (

    select distinct doc_type, source_id
    from {{ source('bronze', 'raw_documents') }}

),

chunked_docs as (

    select distinct doc_type, source_id
    from {{ ref('document_chunks') }}

)

select
    b.doc_type,
    b.source_id
from bronze_docs b
left join chunked_docs c
    on  b.doc_type = c.doc_type
    and b.source_id = c.source_id
where c.source_id is null
