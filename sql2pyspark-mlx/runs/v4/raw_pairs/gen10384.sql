with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) t as key, v
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by host order by status_code asc) as status_code_percent_rank,
        ntile(3) over (partition by host order by status_code asc, id asc, key asc) as status_code_bucket,
        lag(status_code, 2, 0) over (order by status_code asc, id asc, key asc) as prev_status_code
    from flattened
),
kept_keys as (
    -- Set difference on keys
    select host, level
    from scored
    except
    select host, level
    from app_logs
    where level not in ('DEBUG', 'INFO')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join kept_keys k on s.host = k.host and s.level = k.level
),
daily as (
    -- Multi-level aggregation
    select
        host,
        count_if(host = 'h2') as h2_events,
        count(case when level = 'DEBUG' then 1 end) as debug_count,
        avg(status_code_percent_rank) filter (where level = 'DEBUG') as debug_avg_status_code_percent_rank
    from verified
    group by host
)
select
    host,
    h2_events,
    debug_count
from daily
order by debug_count asc nulls first
