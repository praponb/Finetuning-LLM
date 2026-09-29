with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) li as k, v
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by k order by status_code desc) as rank_in_group
    from lines
    qualify rank_in_group = 1
),
ranked_app_logs as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by k order by latency_ms desc, id, k) as rank_in_group_2
    from ranked
    qualify rank_in_group_2 = 1
)
-- Aggregate with conditional metrics
select
    host,
    bit_and(status_code) as status_code_bit_and,
    max(status_code) filter (where host = 'h3') as h3_max_status_code,
    count(*) filter (where host = 'h3') as h3_rows,
    count_if(level = 'WARN') as warn_events,
    count(*) as cnt
from ranked_app_logs
group by host
order by host desc nulls first;
