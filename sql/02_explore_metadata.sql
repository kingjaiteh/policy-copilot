-- ===========================================================================
-- Inspecting the bronze table and its nested metadata
-- ===========================================================================
-- Important first: `metadata` is a STRING column holding JSON, not a nested
-- STRUCT. DESCRIBE will therefore tell you "STRING" and nothing more -- the
-- nested shape is not recorded in the table's schema at all. That was a
-- deliberate trade (it kept the bronze DDL unchanged), and the cost is exactly
-- this: you have to impose the schema at read time.
--
-- Three ways to look inside, cheapest first.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. What the table actually stores
-- ---------------------------------------------------------------------------
DESCRIBE TABLE policy_copilot.bronze.raw_documents;
-- ingestion_id STRING / doc_type STRING / source_id STRING /
-- raw_text STRING / metadata STRING / ingested_at TIMESTAMP

-- DESCRIBE EXTENDED adds size, location, and Delta properties.
DESCRIBE TABLE EXTENDED policy_copilot.bronze.raw_documents;


-- ---------------------------------------------------------------------------
-- 2. Ad-hoc exploration with the `:` path operator
-- ---------------------------------------------------------------------------
-- Databricks' colon operator reads a JSON path straight out of a STRING
-- column, with no schema declared anywhere. Everything it returns is a STRING,
-- so cast with `::` when you want a real type. Field names are case-insensitive.
SELECT
    source_id,
    metadata:system_number::string        AS system_number,
    metadata:component::string            AS component,
    metadata:action_type::string          AS action_type,
    metadata:format_variant::string       AS format_variant,
    metadata:fr_publication_date::date    AS published,
    metadata:parse_status::string         AS parse_status,
    metadata:section_count::int           AS sections,
    metadata:routine_use_count::int       AS routine_uses
FROM policy_copilot.bronze.raw_documents
WHERE doc_type = 'sorn'
ORDER BY source_id;

-- Array elements are indexable, and nested fields chain with dots.
SELECT
    source_id,
    metadata:sections[0].section_key::string  AS first_section,
    metadata:routine_uses[1].label::string    AS second_ru_label,
    left(metadata:routine_uses[1].text::string, 120) AS second_ru_preview
FROM policy_copilot.bronze.raw_documents
WHERE doc_type = 'sorn'
LIMIT 5;

-- Corpus health at a glance -- how the 24 loaded documents broke down.
SELECT
    metadata:parse_status::string   AS parse_status,
    metadata:format_variant::string AS format_variant,
    count(*)                        AS docs,
    sum(metadata:routine_use_count::int) AS routine_uses
FROM policy_copilot.bronze.raw_documents
WHERE doc_type = 'sorn'
GROUP BY ALL
ORDER BY docs DESC;


-- ---------------------------------------------------------------------------
-- 3. Discovering the schema mechanically
-- ---------------------------------------------------------------------------
-- schema_of_json infers a DDL string from a JSON sample. Note that Spark wants
-- a *foldable* (literal) argument, so passing the column directly may be
-- rejected depending on runtime -- paste a sample in if it complains. It also
-- infers from one row only, so an optional field missing from that row will be
-- missing from the schema.
SELECT schema_of_json(metadata) AS inferred_schema
FROM policy_copilot.bronze.raw_documents
WHERE doc_type = 'sorn' AND metadata:parse_status::string = 'ok'
LIMIT 1;

-- Rather than rely on inference, here is the schema written out from the
-- parser's own output, verified across all 24 loaded documents (every one of
-- these 33 keys is present on every document, so nothing here is optional).
-- Use this with from_json when you want typed columns instead of string casts.
CREATE OR REPLACE TEMPORARY VIEW sorn_typed AS
SELECT
    ingestion_id,
    source_id,
    ingested_at,
    raw_text,
    from_json(metadata, '
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
        >') AS m
FROM policy_copilot.bronze.raw_documents
WHERE doc_type = 'sorn';

-- Now the nested fields are real typed columns and tab-completion works.
SELECT source_id, m.system_number, m.action_type, m.fr_volume, m.section_count
FROM sorn_typed
ORDER BY source_id;


-- ---------------------------------------------------------------------------
-- 4. The shape silver actually wants: explode the arrays
-- ---------------------------------------------------------------------------
-- One row per section. This is your chunk table for retrieval -- each row is a
-- self-contained passage with the document metadata needed to filter on.
SELECT
    t.source_id,
    t.m.system_number,
    t.m.component,
    t.m.format_variant,
    s.section_key,
    s.heading,
    s.ordinal,
    s.char_count,
    s.text
FROM sorn_typed t
LATERAL VIEW explode(t.m.sections) AS s
ORDER BY t.source_id, s.ordinal;

-- One row per routine use. This is the classification table: ~370 individual
-- disclosure authorities, each naming a recipient and a purpose.
SELECT
    t.source_id,
    t.m.system_number,
    t.m.component,
    r.label      AS routine_use_label,
    r.char_count,
    size(r.subitems) AS subitem_count,
    r.text
FROM sorn_typed t
LATERAL VIEW explode(t.m.routine_uses) AS r
WHERE r.label <> 'PREAMBLE'
ORDER BY t.source_id, r.label;

-- Sanity check that the explode did not lose anything: these two should agree.
SELECT
    (SELECT sum(m.routine_use_count) FROM sorn_typed)              AS declared,
    (SELECT count(*) FROM sorn_typed t
     LATERAL VIEW explode(t.m.routine_uses) AS r)                  AS exploded;
