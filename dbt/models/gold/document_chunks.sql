{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='chunk_id',
        file_format='delta',
        on_schema_change='sync_all_columns',
        tblproperties={'delta.enableChangeDataFeed': 'true'}
    )
}}

-- The single table the AI Search index reads from.
--
-- WHY incremental + merge, and not the default `table` materialization:
--
--   The default drops and rebuilds. To a vector index syncing off this table,
--   a full overwrite makes the previous state irrelevant, so Databricks
--   re-embeds EVERY row on every dbt run, including the thousands that did
--   not change. With merge plus Change Data Feed, Databricks can see exactly
--   which rows were inserted, updated, or deleted, and recomputes embeddings
--   only for those.
--
--   On Free Edition with one search unit, that is the difference between a
--   fast iteration loop and waiting on a full re-embed every time you tweak
--   a model. Same lesson as the MERGE in the ingestion API, one layer up.
--
--   delta.enableChangeDataFeed is also a hard PREREQUISITE for creating a
--   Delta Sync index on a standard endpoint. It is off by default, which is
--   why it is set here in tblproperties rather than assumed.
--
-- WHY on_schema_change='sync_all_columns': dbt's DEFAULT is 'ignore', which on
-- an incremental model means a newly added column is silently dropped on merge
-- runs. The table keeps its old shape, the new column never appears, and the
-- tests written against it fail with "column not found" rather than anything
-- that points at the cause. Adding chunk_strategy and section_key to this
-- model is exactly that scenario, so the behaviour is made explicit.
--   Note this still needs ONE `dbt build --full-refresh` to backfill the new
--   columns for rows already in the table; sync_all_columns adds the column
--   going forward, it does not recompute history.
--
-- NOTE ON COLUMN NAMING: `_id` is a reserved column name in AI Search indexes
-- and will cause index creation to fail. The key here is `chunk_id`.
--
--
-- CHUNK_STRATEGY: THE A/B HARNESS
-- -------------------------------
-- Free Edition gives one search endpoint and one search unit, so we cannot
-- stand up two indexes to compare chunking strategies. Instead both strategies
-- live in ONE index, tagged, and we filter at query time. Three tag values:
--
--   'standard'        chunks that fit the embedding window. Identical in both
--                     arms, so they are stored once rather than duplicated.
--   'oversized_whole' an over-threshold section kept intact. Arm A only.
--   'sub_split'       that same section broken into overlapping parts. Arm B only.
--
--   Arm A (no splitting):   chunk_strategy != 'sub_split'
--   Arm B (with splitting): chunk_strategy != 'oversized_whole'
--
-- Same index, same embedding model, same questions, one variable changed. Only
-- the oversized sections are duplicated, so the storage cost is small. See
-- eval/README.md for how the arms are actually scored.
--
-- WHY THE THRESHOLD DECIDES WHETHER THIS EXPERIMENT MEANS ANYTHING: at
-- embed_max_chars=2000 (bge-large) 37 sections are oversized and arm B has 109
-- parts to arm A's 37 whole chunks -- a real comparison. At 8000 (gte-large)
-- exactly ONE section in the corpus is oversized and the A/B is measuring
-- noise. If you change the embedding model, re-read whether there is still an
-- experiment here at all.

with sorn_section_chunks as (

    select
        concat('sorn:', source_id, ':sec:', section_key)          as chunk_id,
        'sorn'                                                    as doc_type,
        'section'                                                 as chunk_type,
        -- Tagged on the length of the ASSEMBLED text, not of section_text.
        -- The title/heading prefix below is worth 100-250 characters, so
        -- measuring the bare section misclassifies everything sitting near the
        -- threshold: two sections in this corpus (DHS/CBP-013 and DHS/ICE-002)
        -- land under it on section_text and over it on chunk_text.
        --
        -- Same macro the splitter selects with, so the two cannot disagree
        -- about which sections are oversized.
        case
            when {{ sorn_chunk_is_oversized('document_title', 'agency', 'heading', 'section_text') }}
                then 'oversized_whole'
            else 'standard'
        end                                                       as chunk_strategy,
        source_id,
        section_key,
        document_title,
        -- Prepending the document title and section heading to the embedded
        -- text is deliberate. A chunk reading "Records are retained for six
        -- years" is nearly unretrievable on its own; prefixed with the SORN
        -- title and "Retention and Disposal," it carries the context a
        -- semantic query would actually match against.
        {{ sorn_chunk_text('document_title', 'agency', 'heading', 'section_text') }}
                                                                  as chunk_text,
        heading                                                   as chunk_label,
        section_ordinal                                           as ordinal,
        agency,
        system_number,
        effective_date,
        source_url,
        cast(null as string)                                      as family,
        cast(null as array<string>)                               as baselines,
        content_sha256,
        ingested_at
    from {{ ref('sorn_sections') }}
    -- The routine-uses section is deliberately EXCLUDED here, because
    -- sorn_routine_uses already emits every one of its items as its own chunk
    -- below. Keeping both would index the same prose twice: once as 15-27
    -- individually addressable items, and once as a single 6,000-18,000
    -- character blob. That is not just wasted embedding budget -- the duplicate
    -- competes with its own constituent items for the same query, and the blob
    -- is the weaker match, since averaging 27 disclosure authorities into one
    -- vector blurs the specifics a query like "disclosure to a congressional
    -- office" needs to hit.
    --
    -- KEEP THIS FILTER. It is load-bearing for the A/B, not just for storage:
    -- 9 of the 10 largest sections in the corpus are routine_uses, so letting
    -- them back in would make the oversized population almost entirely
    -- duplicate content that arm B then splits AGAIN into parts competing with
    -- the same items. The experiment would be measuring the duplication, not
    -- the chunking.
    --
    -- Filtered in gold rather than silver: silver stays a complete
    -- representation of the document, gold is the curated view for retrieval.
    -- Same reason bronze keeps raw text you never query directly. But the
    -- SPLITTER has to honour the same list, which is why this is a macro and
    -- not an inline WHERE -- see the note on it in macros/sorn_chunk_text.sql.
    where {{ sorn_section_is_indexable('section_key') }}

),

sorn_routine_use_chunks as (

    select
        concat('sorn:', source_id, ':ru:', routine_use_label)      as chunk_id,
        'sorn'                                                     as doc_type,
        'routine_use'                                              as chunk_type,
        -- Routine uses are already the item-level unit, so they are not part
        -- of the A/B and are always 'standard'. Three of the 347 still cross
        -- the ceiling once subitems are folded in, and those are excluded here
        -- and re-emitted as parts below -- also 'standard', also in both arms.
        'standard'                                                 as chunk_strategy,
        source_id,
        'routine_uses'                                             as section_key,
        document_title,
        concat_ws('\n\n',
            concat(document_title, ' (', coalesce(agency, 'Unknown agency'), ')'),
            concat('Routine Use ', routine_use_label),
            routine_use_full_text
        )                                                          as chunk_text,
        concat('Routine Use ', routine_use_label)                  as chunk_label,
        cast(null as bigint)                                       as ordinal,
        agency,
        system_number,
        effective_date,
        source_url,
        cast(null as string)                                       as family,
        cast(null as array<string>)                                as baselines,
        content_sha256,
        ingested_at
    from {{ ref('sorn_routine_uses') }}
    where not {{ sorn_chunk_is_oversized(
                     'document_title',
                     'agency',
                     "concat('Routine Use ', routine_use_label)",
                     'routine_use_full_text') }}

),

sorn_routine_use_split_chunks as (

    -- The three over-ceiling routine uses, as parts. See
    -- silver/sorn_routine_uses_split.sql for why folding subitems in pushed
    -- them over a limit the item text alone stays well under.
    select
        concat('sorn:', source_id, ':ru:', routine_use_label, ':p', part_no) as chunk_id,
        'sorn'                                                     as doc_type,
        'routine_use'                                              as chunk_type,
        'standard'                                                 as chunk_strategy,
        source_id,
        'routine_uses'                                             as section_key,
        document_title,
        {{ sorn_chunk_text(
               'document_title',
               'agency',
               "concat('Routine Use ', routine_use_label, ' (part ', part_no + 1, ' of ', total_parts, ')')",
               'part_text'
           ) }}                                                    as chunk_text,
        concat('Routine Use ', routine_use_label,
               ' (part ', part_no + 1, ' of ', total_parts, ')')   as chunk_label,
        cast(null as bigint)                                       as ordinal,
        agency,
        system_number,
        effective_date,
        source_url,
        cast(null as string)                                       as family,
        cast(null as array<string>)                                as baselines,
        content_sha256,
        ingested_at
    from {{ ref('sorn_routine_uses_split') }}

),

sorn_sub_split_chunks as (

    -- Arm B's replacement for the oversized_whole chunks above. Same source
    -- text, broken into overlapping parts by silver/sorn_sections_split.
    select
        concat('sorn:', source_id, ':sec:', section_key, ':p', part_no)  as chunk_id,
        'sorn'                                                     as doc_type,
        'section'                                                  as chunk_type,
        'sub_split'                                                as chunk_strategy,
        source_id,
        section_key,
        document_title,
        {{ sorn_chunk_text(
               'document_title',
               'agency',
               "concat(heading, ' (part ', part_no + 1, ' of ', total_parts, ')')",
               'part_text'
           ) }}                                                    as chunk_text,
        -- Part numbering goes in the heading so a retrieved fragment announces
        -- that it is one piece of a longer section, rather than looking like
        -- the section's complete text.
        concat(heading, ' (part ', part_no + 1, ' of ', total_parts, ')') as chunk_label,
        section_ordinal                                            as ordinal,
        agency,
        system_number,
        effective_date,
        source_url,
        cast(null as string)                                       as family,
        cast(null as array<string>)                                as baselines,
        content_sha256,
        ingested_at
    from {{ ref('sorn_sections_split') }}

),

nist_chunks as (

    -- Controls that fit the embedding window, kept whole. One control is one
    -- chunk: splitting a control that fits would separate a requirement from
    -- its discussion, which is exactly the pairing that makes it useful to
    -- retrieve.
    select
        concat('nist:', source_id)                                 as chunk_id,
        'nist_control'                                             as doc_type,
        case when is_enhancement then 'control_enhancement' else 'control' end as chunk_type,
        -- NIST is NOT part of the A/B. Everything here is 'standard', so it is
        -- present and identical in both arms and acts as shared ballast: every
        -- question competes against the same NIST corpus either way, so a
        -- difference between arms can never be caused by these rows.
        'standard'                                                 as chunk_strategy,
        source_id,
        cast(null as string)                                       as section_key,
        title                                                      as document_title,
        raw_text                                                   as chunk_text,
        concat(source_id, ' ', title)                              as chunk_label,
        cast(null as bigint)                                       as ordinal,
        cast(null as string)                                       as agency,
        cast(null as string)                                       as system_number,
        cast(null as string)                                       as effective_date,
        source_url,
        family,
        baselines,
        cast(null as string)                                       as content_sha256,
        ingested_at
    from {{ ref('stg_nist_control') }}
    where not parse_failed
      -- The 27 controls over the ceiling are excluded here and re-emitted as
      -- parts below. They are NOT tagged 'oversized_whole': that tag means "the
      -- control arm of the experiment," and NIST is not in the experiment.
      and length(raw_text) <= {{ var('embed_max_chars') }}

),

nist_split_chunks as (

    -- The over-ceiling controls, as parts. Also 'standard', also in both arms.
    select
        concat('nist:', source_id, ':p', part_no)                  as chunk_id,
        'nist_control'                                             as doc_type,
        case when is_enhancement then 'control_enhancement' else 'control' end as chunk_type,
        'standard'                                                 as chunk_strategy,
        source_id,
        cast(null as string)                                       as section_key,
        title                                                      as document_title,
        -- Unlike a SORN section, raw_text already opens with the control id
        -- and title, so only the part marker needs prepending -- and only for
        -- parts after the first, which still carry that opening line.
        concat_ws('\n\n',
            concat(source_id, ' ', title,
                   ' (part ', part_no + 1, ' of ', total_parts, ')'),
            part_text
        )                                                          as chunk_text,
        concat(source_id, ' ', title,
               ' (part ', part_no + 1, ' of ', total_parts, ')')   as chunk_label,
        cast(null as bigint)                                       as ordinal,
        cast(null as string)                                       as agency,
        cast(null as string)                                       as system_number,
        cast(null as string)                                       as effective_date,
        source_url,
        family,
        baselines,
        cast(null as string)                                       as content_sha256,
        ingested_at
    from {{ ref('nist_controls_split') }}

),

unioned as (

    -- Columns listed explicitly rather than `select *`. UNION ALL matches by
    -- POSITION, not name, so a `select *` union silently misaligns the moment
    -- one branch gains a column or reorders one. Naming them makes a mismatch
    -- a compile error instead of corrupt data.
    select
        chunk_id, doc_type, chunk_type, chunk_strategy, source_id, section_key,
        document_title, chunk_text, chunk_label, ordinal, agency, system_number,
        effective_date, source_url, family, baselines, content_sha256, ingested_at
    from sorn_section_chunks

    union all

    select
        chunk_id, doc_type, chunk_type, chunk_strategy, source_id, section_key,
        document_title, chunk_text, chunk_label, ordinal, agency, system_number,
        effective_date, source_url, family, baselines, content_sha256, ingested_at
    from sorn_sub_split_chunks

    union all

    select
        chunk_id, doc_type, chunk_type, chunk_strategy, source_id, section_key,
        document_title, chunk_text, chunk_label, ordinal, agency, system_number,
        effective_date, source_url, family, baselines, content_sha256, ingested_at
    from sorn_routine_use_chunks

    union all

    select
        chunk_id, doc_type, chunk_type, chunk_strategy, source_id, section_key,
        document_title, chunk_text, chunk_label, ordinal, agency, system_number,
        effective_date, source_url, family, baselines, content_sha256, ingested_at
    from sorn_routine_use_split_chunks

    union all

    select
        chunk_id, doc_type, chunk_type, chunk_strategy, source_id, section_key,
        document_title, chunk_text, chunk_label, ordinal, agency, system_number,
        effective_date, source_url, family, baselines, content_sha256, ingested_at
    from nist_chunks

    union all

    select
        chunk_id, doc_type, chunk_type, chunk_strategy, source_id, section_key,
        document_title, chunk_text, chunk_label, ordinal, agency, system_number,
        effective_date, source_url, family, baselines, content_sha256, ingested_at
    from nist_split_chunks

)

select
    *,
    length(chunk_text) as chunk_char_count
from unioned
where chunk_text is not null
  and length(trim(chunk_text)) > 0

{% if is_incremental() %}
  -- On incremental runs, only process rows newer than what we already have.
  -- Because the ingestion API MERGEs and refreshes ingested_at on update, an
  -- amended document naturally reappears here and updates its chunks in place.
  --
  -- CAVEAT worth knowing before you tweak a model and wonder why nothing
  -- changed: this filter keys on ingestion time, not on model logic. Editing
  -- the chunking SQL does not change any ingested_at, so an incremental run
  -- will not rewrite existing chunks. A logic change needs --full-refresh.
  and ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
