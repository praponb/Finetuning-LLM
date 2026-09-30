with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) e as k, attr_value
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        host,
        log_ts,
        level,
        attr_value,
        case when level = 'DEBUG' then 1 else 0 end as is_debug,
        coalesce(k, 'N/A') as resolved_k
    from exploded
    where log_ts >= date '2025-06-01'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        host,
        log_ts,
        level,
        attr_value,
        is_debug,
        resolved_k,
        percent_rank() over (order by is_debug) as is_debug_percent_rank,
        max(is_debug) over (partition by host, level order by cast(log_ts as bigint) range between 900 preceding and current row) as is_debug_max_15m,
        avg(is_debug) over w_running as is_debug_avg_2h,
        count(is_debug) over (partition by host, level) as host_count_is_debug,
        dense_rank() over (partition by level, host order by is_debug desc) as is_debug_dense_rank
    from cleaned
    window w_running as (partition by host order by cast(log_ts as bigint) range between 7200 preceding and current row)
)
-- Aggregate with conditional metrics
select
    date_trunc('day', log_ts) as activity_day,
    date_trunc('hour', log_ts) as hour_window,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as span_minutes,
    count_if(level = 'WARN') as warn_events,
    min(log_ts) as first_seen,
    round(avg(is_debug_max_15m), 2) as avg_is_debug_max_15m,
    sum(is_debug) filter (where host = 'h3') as h3_total_is_debug,
    count_if(host = 'h1') as h1_events
from metrics
group by date_trunc('day', log_ts), date_trunc('hour', log_ts)
having round(avg(is_debug_max_15m), 2) < 0
