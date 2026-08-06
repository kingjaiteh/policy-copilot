-- Parses NIST 800-53 control metadata out of bronze.
--
-- Separate model from stg_sorn because the two corpora have genuinely
-- different shapes. Forcing them into one schema would mean a wide table
-- that is mostly NULL, and you would lose the type safety that makes the
-- explode in sorn_sections work.
--
-- The two are reunited in gold/document_chunks on a small common contract.

with raw as (

    select
        ingestion_id,
        source_id,
        raw_text,
        ingested_at,
        from_json(
            metadata,
            '
            STRUCT<
                title: STRING, control_id: STRING,
                family_id: STRING, family: STRING,
                is_enhancement: BOOLEAN, parent_control: STRING,
                related_controls: ARRAY<STRING>,
                baselines: ARRAY<STRING>,
                source: STRING, source_url: STRING, char_count: BIGINT
            >'
        ) as m
    from {{ source('bronze', 'raw_documents') }}
    where doc_type = 'nist_control'

)

select
    ingestion_id,
    source_id,              -- human label, e.g. 'AC-2' or 'AC-2(1)'
    raw_text,
    ingested_at,

    m.title,
    m.control_id,           -- OSCAL id, e.g. 'ac-2.1'
    m.family_id,
    m.family,
    m.is_enhancement,
    m.parent_control,
    m.related_controls,
    m.baselines,
    m.source,
    m.source_url,

    -- Convenience booleans. Cheaper to filter on than array_contains in
    -- every downstream query, and they read better in the agent's tool schema.
    array_contains(m.baselines, 'low')      as in_low_baseline,
    array_contains(m.baselines, 'moderate') as in_moderate_baseline,
    array_contains(m.baselines, 'high')     as in_high_baseline,

    m is null as parse_failed

from raw
