-- One row per routine use.
--
-- Routine uses get their own model rather than living inside the generic
-- sections explode, for two reasons:
--
--   1. They are the single most-asked-about part of a SORN. "Can this agency
--      share this data with X" is a routine-use question. Making them
--      individually retrievable means the agent cites the specific lettered
--      routine use rather than dumping the whole section.
--   2. They nest one level deeper (subitems), so they need their own
--      flattening logic anyway.

select
    s.source_id,
    s.title        as document_title,
    s.agency,
    s.system_number,
    s.effective_date,
    s.source_url,
    s.content_sha256,

    ru.label       as routine_use_label,
    ru.text        as routine_use_text,
    ru.char_count  as routine_use_char_count,

    -- Subitems are lettered sub-clauses under a routine use. We fold them into
    -- the parent text rather than exploding again: they are usually one clause
    -- long, and standalone they lack the context to be retrievable on their own.
    case
        when ru.subitems is null or size(ru.subitems) = 0 then ru.text
        else concat(
            ru.text,
            '\n',
            array_join(transform(ru.subitems, x -> concat(x.label, ' ', x.text)), '\n')
        )
    end as routine_use_full_text,

    size(coalesce(ru.subitems, array())) as subitem_count,

    s.ingested_at

from {{ ref('stg_sorn') }} as s
lateral view explode(s.routine_uses) as ru

where not s.parse_failed
  and ru.text is not null
  and length(trim(ru.text)) > 0
