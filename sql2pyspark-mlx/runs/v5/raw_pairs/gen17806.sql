with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) t as k, v
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by level order by status_code desc, id, k) as rn
    from flattened
    qualify rn = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(k) ignore nulls over (partition by k order by status_code, id, k rows between unbounded preceding and current row) last_known_k,
        avg(status_code) over (partition by k, level order by status_code, id, k rows between 5 preceding and current row) status_code_avg_5
    from deduped
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when log_ts - lag(log_ts) over (partition by k order by log_ts, id, k) > interval 45 minutes then 1 else 0 end as new_group_flag
    from enriched
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as island_id
    from flagged
)
select
    id,
    host,
    log_ts,
    level,
    k,
    v,
    new_group_flag,
    island_id,
    case when level rlike '^(INFO|WARN)$' then 'CORE' when level rlike '^(ERROR|DEBUG)$' then 'GROUP_A' else 'EXTENDED' end as level_zone
from islands
order by new_group_flag desc nulls first
