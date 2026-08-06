{#
    Make the +schema settings in dbt_project.yml ABSOLUTE rather than a suffix.

    dbt's built-in generate_schema_name CONCATENATES: with `schema: silver` in
    profiles.yml and `+schema: gold` on the folder, the default macro produces
    `silver_gold`, not `gold`. Silver models likewise land in `silver_silver`.
    The suffixing default exists so several developers can build into the same
    warehouse without colliding -- each gets <their_schema>_<custom>.

    That is not what this project wants. The bronze/silver/gold names are fixed
    parts of the model, the vector index is created against a specific
    `policy_copilot.gold.document_chunks`, and there is one developer. So we
    override to treat a custom schema as the literal target.

    Consequence to be aware of: every target now writes to the same schemas, so
    if a `prod` target is ever added it will build over `dev` output. At that
    point branch on target.name here rather than reverting to the suffixing
    default.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
