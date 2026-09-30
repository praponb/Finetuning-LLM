with lines as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) t as attr_key, v
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        level,
        status_code,
        v,
        cast((status_code & 1) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from lines
    where level = 'WARN'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        sum(is_flagged) over w_running as level_sum_is_flagged,
        percent_rank() over (order by is_flagged desc) as is_flagged_percent_rank
    from parsed
    window w_running as (partition by level)
),
daily as (
    -- Multi-level aggregation
    select
        level,
        count(*) filter (where level = 'WARN') as warn_rows,
        approx_percentile(status_code, array(0.25, 0.5, 0.75)) as status_code_quartiles,
        grouping(level) as grouping_id
    from enriched
    group by rollup (level)
)
select
    level,
    warn_rows,
    grouping_id
from daily
order by warn_rows;
