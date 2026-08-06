-- Parses the SORN metadata JSON blob out of bronze.
--
-- THIS IS THE ONLY PLACE the SORN struct schema is written down. Every
-- downstream model does `ref('stg_sorn')` and gets typed columns, so the
-- 30-line STRUCT<...> literal never gets copy-pasted or drifts.
--
-- from_json failure modes worth knowing:
--   * malformed JSON            -> the whole struct comes back NULL
--   * field present in schema, absent in JSON -> that field is NULL
--   * field present in JSON, absent from schema -> silently DROPPED
-- None of these raise. The `parse_failed` flag below catches the first case;
-- the schema_drift test in _models.yml catches it as a hard failure.

with raw as (

    select
        ingestion_id,
        source_id,
        raw_text,
        ingested_at,
        metadata,
        from_json(
            metadata,
            '
            STRUCT<
                title: STRING, system_number: STRING, agency: STRING, component: STRING,
                source_url: STRING, source_file: STRING, docket_number: STRING,
                fr_doc_number: STRING, fr_volume: BIGINT, fr_number: BIGINT,
                fr_pages: STRING, fr_publication_date: STRING, effective_date: STRING,
                action_raw: STRING, action_type: STRING, agency_line: STRING,
                summary: STRING, dates_raw: STRING, contact_raw: STRING,
                security_classification: STRING,
                fr_citations: ARRAY<STRING>, page_markers: ARRAY<STRING>,
                format_variant: STRING, content_sha256: STRING, char_count: BIGINT,
                parser_version: STRING, parse_status: STRING,
                quality_flags: ARRAY<STRING>, missing_sections: ARRAY<STRING>,
                section_count: BIGINT, routine_use_count: BIGINT,
                sections: ARRAY<STRUCT<
                    section_key: STRING, heading: STRING, ordinal: BIGINT,
                    text: STRING, char_count: BIGINT>>,
                routine_uses: ARRAY<STRUCT<
                    label: STRING, text: STRING, char_count: BIGINT,
                    subitems: ARRAY<STRUCT<label: STRING, text: STRING>>>>
            >'
        ) as m
    from {{ source('bronze', 'raw_documents') }}
    where doc_type = 'sorn'

)

select
    ingestion_id,
    source_id,
    raw_text,
    ingested_at,

    -- Flat scalar columns, promoted out of the struct for easy filtering.
    m.title,
    m.system_number,
    m.agency,
    m.component,
    m.source_url,
    m.docket_number,
    m.fr_doc_number,
    m.fr_publication_date,
    m.effective_date,
    m.action_type,
    m.summary,
    m.security_classification,
    m.format_variant,
    m.content_sha256,
    m.parser_version,
    m.parse_status,
    m.quality_flags,
    m.missing_sections,
    m.section_count,
    m.routine_use_count,

    -- Nested arrays kept intact; exploded in the models downstream.
    m.sections,
    m.routine_uses,

    -- Distinguishes "the JSON did not match the schema at all" from
    -- "the parser itself reported a problem." Different fixes.
    m is null as parse_failed

from raw
