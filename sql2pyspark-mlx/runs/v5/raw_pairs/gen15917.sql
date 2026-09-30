with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) x as key, v
),
parsed as (
    -- Semi-structured parsing
    select
        id,
        key,
        v,
        cast((status_code & 4) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from flattened
),
scored as (
    -- Ranking and gap features
    select
        id,
        key,
        sum(has_fault) over w_part as cumulative_has_fault,
        min(has_fault) over w_part_2 as key_min_has_fault,
        row_number() over w_running as row_seq,
        percent_rank() over (partition by key order by status_code_high desc nulls first) as status_code_high_percent_rank,
        row_number() over (order by status_code_high desc, id, key) as row_seq_2
    from parsed
    where has_fault <= 1
    window w_part as (partition by key order by has_fault, id, key rows between unbounded preceding and current row), w_part_2 as (partition by key), w_running as (partition by key order by has_fault desc, id, key)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        key,
        row_seq,
        status_code_high_percent_rank
    from scored
    qualify row_number() over (partition by key order by key_min_has_fault nulls first, id, key) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(row_seq) over (partition by key order by row_seq, id, key rows between unbounded preceding and current row) as running_sum_row_seq,
        count(row_seq) over (partition by key order by row_seq, id, key rows between unbounded preceding and current row) as running_count_row_seq,
        max(row_seq) over (partition by key order by row_seq, id, key rows between unbounded preceding and current row) as running_max_row_seq,
        avg(status_code_high_percent_rank) over (partition by key order by status_code_high_percent_rank, id, key rows between 6 preceding and current row) as status_code_high_percent_rank_avg_6
    from deduped
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        key,
        running_sum_row_seq,
        status_code_high_percent_rank
    from enriched
) src
unpivot include nulls (
    value for metric in (running_sum_row_seq, status_code_high_percent_rank)
)
