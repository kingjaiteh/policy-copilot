{#
    The exact string that gets embedded for a SORN chunk.

    WHY THIS IS A MACRO AND NOT JUST INLINE SQL

    Two different models need to agree, to the character, on how long a chunk
    will be:

      * gold/document_chunks.sql builds the text and tags each chunk
        'standard' or 'oversized_whole' based on its length.
      * silver/sorn_sections_split.sql decides which sections to split, and
        must select EXACTLY the sections gold tags as oversized.

    If those two predicates ever disagree, the A/B breaks in a way no test
    would obviously catch: a section could be tagged 'oversized_whole' in gold
    while the splitter skipped it, leaving arm B with a hole, or the splitter
    could emit parts for a section gold considers standard, double-indexing it
    in both arms.

    Writing the length predicate as `length(section_text) > N` is the version
    of this bug that is easy to introduce, because the prepended title and
    heading are invisible when you are looking at the section text. They are
    worth 100-250 characters, which is exactly the band where sections sit
    around a 2,000 character threshold.

    So: one definition, called from both sides. The length predicate is
    `length(sorn_chunk_text(...)) > embed_max_chars`, never a bare column.

    NOTE ON concat_ws AND NULLS: concat_ws SKIPS null arguments rather than
    propagating them, so a null heading shortens the string instead of nulling
    it. That is the behaviour we want, and it is another reason to measure the
    assembled string rather than reconstruct its length arithmetically.
#}

{% macro sorn_chunk_text(document_title, agency, heading, body) %}
concat_ws('\n\n',
    concat({{ document_title }}, ' (', coalesce({{ agency }}, 'Unknown agency'), ')'),
    {{ heading }},
    {{ body }}
)
{%- endmacro %}


{#
    True when a SORN section is too long to embed without silent truncation.

    Embedding models do not error on over-length input, they truncate it, so an
    oversized chunk loses its tail with no signal anywhere. `embed_max_chars`
    must therefore track whichever model the vector index is configured with;
    see the note on it in dbt_project.yml.
#}

{% macro sorn_chunk_is_oversized(document_title, agency, heading, body) %}
length({{ sorn_chunk_text(document_title, agency, heading, body) }}) > {{ var('embed_max_chars') }}
{%- endmacro %}


{#
    The sections gold actually indexes.

    WHY THIS IS A MACRO TOO, learned the expensive way: this predicate first
    existed only in gold's WHERE clause, while silver/sorn_sections_split
    selected from sorn_sections filtered by size alone. The splitter therefore
    happily split the sections gold throws away -- routine_uses above all,
    which is over the ceiling in all 23 documents.

    The result got as far as a real build: 275 sub_split chunks instead of 109,
    166 of them parts of a routine_uses blob that gold never emits whole. Arm B
    ended up with fragments of the routine-uses section competing against the
    very item-level routine use chunks that section was excluded in favour of,
    and assert_sub_split_covers_oversized failed with 23 orphans.

    So the rule is the same as for the length predicate: one definition, called
    from both sides. A filter that exists in only one of two models that must
    agree is a bug waiting for a corpus large enough to show it.
#}

{% macro sorn_section_is_indexable(section_key='section_key') %}
(
    -- Routine uses are re-emitted item by item by sorn_routine_uses, so the
    -- whole-section version is dropped rather than indexed twice.
    {{ section_key }} != 'routine_uses'
    -- Metadata restating typed columns. About 13 characters each, so never the
    -- right hit, but they still occupy top-k slots and dilute the result.
    and {{ section_key }} not in (
        'system_name_and_number',
        'system_name',
        'security_classification'
    )
)
{%- endmacro %}
