-- SINGULAR TEST (CRITICAL)
--
-- Every 'oversized_whole' chunk must have at least one matching 'sub_split'
-- part, and vice versa. Otherwise the two arms are not comparing the same
-- content.
--
--   Missing parts:  arm B's filter (chunk_strategy != 'oversized_whole')
--                   removes the whole section and nothing replaces it, so that
--                   content is simply absent from arm B.
--   Orphan parts:   arm A's filter removes the parts and nothing keeps the
--                   whole section, same hole in the other direction.
--
-- Either way the comparison silently becomes "with content vs without content"
-- rather than "unsplit vs split," and one arm looks worse for a reason that
-- has nothing to do with chunking. An experiment that still produces numbers
-- while measuring the wrong thing is worse than one that fails loudly.
--
-- Joins on the section_key COLUMN rather than regexp_extract-ing it back out
-- of chunk_id. The parsed-from-the-key version was quietly fragile: the
-- greedy '^sorn:.*:sec:(.*)$' also matches a sub_split id, capturing
-- 'routine_uses:p3' as if it were a section key, so the two sides of the join
-- could never match even when coverage was fine. Carrying section_key as a
-- real column in gold removes the parsing entirely.

with oversized as (
    select source_id, section_key
    from {{ ref('document_chunks') }}
    where chunk_strategy = 'oversized_whole'
),

parts as (
    select distinct source_id, section_key
    from {{ ref('document_chunks') }}
    where chunk_strategy = 'sub_split'
)

select
    coalesce(o.source_id, p.source_id)   as source_id,
    coalesce(o.section_key, p.section_key) as section_key,
    case
        when p.section_key is null then 'oversized section has no sub_split parts'
        else 'sub_split parts have no oversized section'
    end                                   as problem
from oversized o
full outer join parts p
    on  o.source_id   = p.source_id
    and o.section_key = p.section_key
where o.section_key is null
   or p.section_key is null
