with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        k,
        cast((status_code & 1) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from flattened
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        host,
        k,
        is_flagged,
        status_code_high,
        round(is_flagged * 0.85, 2) as is_flagged_adjusted,
        coalesce(k, 'NONE') as resolved_k
    from parsed
    where k is not null
      and k <> 'version'
),
scored as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by host, k order by status_code_high) as status_code_high_percent_rank,
        count(status_code_high) over (partition by k) as k_count_status_code_high,
        min(is_flagged_adjusted) over (partition by k) as k_min_is_flagged_adjusted
    from valid
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        k,
        is_flagged,
        status_code_high,
        is_flagged_adjusted,
        resolved_k,
        status_code_high_percent_rank,
        k_count_status_code_high,
        k_min_is_flagged_adjusted,
        least(status_code_high_percent_rank, k_count_status_code_high) as least_status_code_high_percent_rank_k_count_status_code_high,
        coalesce(k, 'DIRECT') as resolved_k_2
    from scored
    where is_flagged_adjusted <= 25
)
-- Running and moving aggregates
select
    id,
    k,
    is_flagged_adjusted,
    status_code_high_percent_rank,
    k_min_is_flagged_adjusted,
    least_status_code_high_percent_rank_k_count_status_code_high,
    resolved_k_2,
    rank() over (order by status_code_high desc nulls first) as status_code_high_rank,
    percent_rank() over (partition by k order by is_flagged_adjusted desc nulls first) as is_flagged_adjusted_percent_rank,
    cume_dist() over (order by k_count_status_code_high desc nulls last) as k_count_status_code_high_cume_dist,
    avg(status_code_high_percent_rank) over (partition by k) as k_avg_status_code_high_percent_rank,
    rank() over (partition by k order by is_flagged_adjusted desc) as is_flagged_adjusted_rank
from base
