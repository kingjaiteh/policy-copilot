{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='chunk_id',
        file_format='delta',
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
-- NOTE ON COLUMN NAMING: `_id` is a reserved column name in AI Search indexes
-- and will cause index creation to fail. The key here is `chunk_id`.

with sorn_section_chunks as (

    select
        concat('sorn:', source_id, ':sec:', section_key)          as chunk_id,
        'sorn'                                                    as doc_type,
        'section'                                                 as chunk_type,
        source_id,
        document_title,
        -- Prepending the document title and section heading to the embedded
        -- text is deliberate. A chunk reading "Records are retained for six
        -- years" is nearly unretrievable on its own; prefixed with the SORN
        -- title and "Retention and Disposal," it carries the context a
        -- semantic query would actually match against.
        concat_ws('\n\n',
            concat(document_title, ' (', coalesce(agency, 'Unknown agency'), ')'),
            heading,
            section_text
        )                                                         as chunk_text,
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

),

sorn_routine_use_chunks as (

    select
        concat('sorn:', source_id, ':ru:', routine_use_label)      as chunk_id,
        'sorn'                                                     as doc_type,
        'routine_use'                                              as chunk_type,
        source_id,
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

),

nist_chunks as (

    -- No explode here. NIST controls average well under 1,000 characters, so
    -- one control is already one chunk. Splitting them would separate a
    -- requirement from its discussion, which is exactly the pairing that makes
    -- them useful to retrieve.
    select
        concat('nist:', source_id)                                 as chunk_id,
        'nist_control'                                             as doc_type,
        case when is_enhancement then 'control_enhancement' else 'control' end as chunk_type,
        source_id,
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

),

unioned as (
    select * from sorn_section_chunks
    union all
    select * from sorn_routine_use_chunks
    union all
    select * from nist_chunks
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
  and ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
