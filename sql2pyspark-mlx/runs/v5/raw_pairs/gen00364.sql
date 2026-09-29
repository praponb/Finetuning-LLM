with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) li as key, attr_value
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', log_ts) as hour_window,
    round(avg(status_code), 2) as avg_status_code,
    min(log_ts) as first_seen,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min
from lines
group by hour_window;
