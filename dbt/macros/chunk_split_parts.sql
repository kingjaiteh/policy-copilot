{#
    Splits over-length text into overlapping, embedding-sized parts.

    Emits: <keys...>, part_no (0-based, contiguous), total_parts, part_text,
    part_char_count. One row per part; rows whose text already fits are not
    selected in the first place, so they never appear.

    Called by silver/sorn_sections_split.sql and silver/nist_controls_split.sql.
    It is a macro because both corpora hit the same ceiling and the algorithm
    below is about 80 lines of non-obvious SQL -- two copies would drift, and
    the failure mode when they drift is silent truncation, which is the exact
    thing this code exists to prevent.

    ARGUMENTS
      relation         a ref() to the model being split
      keys             list of column names uniquely identifying one text unit
      text_column      the column holding the text to split
      where_clause     SQL predicate selecting the over-length rows


    WHY THREE TIERS OF SPLITTING

      Each tier catches what the one above it cannot, and every tier is load
      bearing on this corpus:

        1. line      split on '\n'         best boundary, when one exists
        2. sentence  split on '. ' etc.    for text with no line breaks at all
        3. hard      fixed character cut   guarantees termination regardless

      Tier 1 alone is not enough. scripts/sorn_parser.py:reflow() UNWRAPS
      hard-wrapped GPO captures, so a pre-2016 SORN section with no blank lines
      collapses to exactly one line: of the twelve largest sections in the
      corpus, five are a single line, the longest 17,739 characters. Splitting
      those on '\n' does nothing at all.

      Tier 3 is not expected to fire today (the longest tier-2 atom measures
      571 characters) but it is what makes the macro total. Without it a single
      unpunctuated run longer than the target would emit one over-length part
      and quietly reintroduce truncation.

      A previous implementation split on '\n\n' and, because nothing in this
      pipeline ever emits a blank line, returned every section unchanged while
      still producing correctly-shaped rows. See
      tests/assert_sub_split_actually_splits.sql.
#}

{% macro chunk_split_parts(relation, keys, text_column, where_clause) %}

{%- set key_list = keys | join(', ') -%}

with oversized as (

    select *
    from {{ relation }}
    where {{ where_clause }}

),

lines as (

    -- TIER 1. posexplode gives the element AND its position. The position is
    -- what keeps everything in document order through the aggregation below,
    -- since collect_list makes no ordering guarantee whatsoever.
    select
        {{ key_list }},
        l.pos  as line_pos,
        l.line as line
    from oversized
    lateral view posexplode(split({{ text_column }}, '\n')) l as pos, line
    where length(trim(l.line)) > 0

),

sentences as (

    -- TIER 2. Split after sentence-ending punctuation.
    --
    -- The lookbehind requires the character before the stop to be lowercase, a
    -- digit, or a closing bracket. That is what stops "DHS/CBP-011 U.S.
    -- Customs" splitting after "U.S." and "Docket No. 5" after "No." -- both
    -- are everywhere in this corpus and both are preceded by a capital. Java,
    -- and so Spark, permits the bounded lookbehind this needs.
    --
    -- Over-splitting here is harmless: the packer reassembles consecutive
    -- sentences up to the target, so tier 2 only changes WHERE a part may
    -- break, never how large it ends up.
    select
        {{ key_list }},
        line_pos,
        s.pos  as sent_pos,
        s.sent as sent
    from lines
    lateral view posexplode(split(line, '(?<=[a-z0-9)\\]][.?!])\\s+')) s as pos, sent
    where length(trim(s.sent)) > 0

),

atoms as (

    -- TIER 3, and where separators get baked in.
    --
    -- sequence(0, floor((len-1)/cap)) yields [0] for anything already under
    -- the cap, so the common case passes through untouched and there is no
    -- conditional to get wrong.
    --
    -- Each atom carries its own LEADING separator: '\n' if it starts a new
    -- source line, ' ' if it continues one. Baking the separator into the atom
    -- makes the packer a plain concat, and makes the reassembled part
    -- reproduce the original line structure rather than flattening every
    -- break to a space.
    select
        {{ key_list }},
        line_pos,
        sent_pos,
        c.piece_idx,
        case
            when sent_pos = 0 and c.piece_idx = 0 and line_pos > 0 then '\n'
            else ' '
        end
        || substring(sent, c.piece_idx * {{ var('split_target_chars') }} + 1,
                           {{ var('split_target_chars') }}) as atom
    from sentences
    lateral view explode(
        sequence(0, cast(floor((length(sent) - 1) / {{ var('split_target_chars') }}) as int))
    ) c as piece_idx

),

ordered as (

    -- One ordered array<string> of atoms per text unit. array_sort on a struct
    -- orders by its fields left to right, which is why all three positions are
    -- carried this far.
    select
        {{ key_list }},
        transform(
            array_sort(collect_list(struct(line_pos, sent_pos, piece_idx, atom))),
            x -> x.atom
        ) as atoms
    from atoms
    group by {{ key_list }}

),

packed as (

    -- GREEDY PACK, in pure SQL, via the higher-order function `aggregate`.
    --
    -- WHY NOT THE OBVIOUS WINDOW-FUNCTION VERSION: the tempting approach is a
    -- running SUM of atom lengths and floor(cum_len / target) as the part
    -- number. It is shorter and it is wrong twice over.
    --
    --   1. Part sizes are unbounded above. The atom that crosses a bucket
    --      boundary lands wholly in one bucket, so a part can reach
    --      target + (longest atom) -- at target=1200 that is 2,399 characters,
    --      back over the ceiling this macro exists to respect.
    --   2. Part numbers come out non-contiguous. An atom spanning several
    --      buckets skips those indices, so a unit can emit a single row
    --      numbered part_no=5 while total_parts=1, labelled "part 6 of 1".
    --
    -- `aggregate` folds left over the ordered array carrying (finished parts,
    -- current part) and starts a new part when the next atom would overflow,
    -- which is the actual greedy algorithm. Parts are then bounded by
    -- split_target_chars, and posexplode numbers them contiguously from zero
    -- by construction.
    select
        {{ key_list }},
        aggregate(
            atoms,
            named_struct('parts', cast(array() as array<string>), 'cur', cast('' as string)),
            (acc, atom) ->
                case
                    -- First atom of a part: drop its leading separator.
                    --
                    -- substring(atom, 2) and NOT trim(atom): Spark's trim()
                    -- removes space characters only, not newlines, so an atom
                    -- beginning a source line would keep its '\n' and every
                    -- such part would open with a blank line. Every atom
                    -- carries exactly one separator character, so slicing past
                    -- it is both exact and cheaper.
                    when acc.cur = ''
                        then named_struct('parts', acc.parts, 'cur', substring(atom, 2))
                    when length(acc.cur) + length(atom) <= {{ var('split_target_chars') }}
                        then named_struct('parts', acc.parts, 'cur', concat(acc.cur, atom))
                    else named_struct(
                             'parts', array_append(acc.parts, acc.cur),
                             'cur',   substring(atom, 2)
                         )
                end,
            -- Flush whatever is still in progress when the fold runs out.
            acc -> case
                       when acc.cur = '' then acc.parts
                       else array_append(acc.parts, acc.cur)
                   end
        ) as parts
    from ordered

),

exploded as (

    select
        {{ key_list }},
        p.part_no,
        p.part_text
    from packed
    lateral view posexplode(parts) p as part_no, part_text

),

lagged as (

    -- Tail of the PREVIOUS part. lag reads part_text before any overlap was
    -- applied, so overlaps never compound down a long unit.
    select
        {{ key_list }},
        part_no,
        part_text,
        count(*) over (partition by {{ key_list }}) as total_parts,
        right(
            lag(part_text) over (partition by {{ key_list }} order by part_no),
            {{ var('split_overlap_chars') }}
        ) as prev_tail
    from exploded

),

overlapped as (

    select
        {{ key_list }},
        part_no,
        total_parts,
        case
            when prev_tail is null then part_text
            else concat(
                -- right() cuts mid-word, so drop everything up to the first
                -- space to snap forward to a word boundary. Worst case that
                -- SHORTENS the overlap; it never lengthens it, which is what
                -- keeps the size bound asserted in _models.yml honest.
                case
                    when instr(prev_tail, ' ') > 0
                        then substring(prev_tail, instr(prev_tail, ' ') + 1)
                    else prev_tail
                end,
                '\n',
                part_text
            )
        end as part_text
    from lagged

)

select
    {{ key_list }},
    part_no,
    total_parts,
    part_text,
    length(part_text) as part_char_count
from overlapped

{% endmacro %}
