with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
filtered as (
    select
        service,
        host,
        log_ts,
        latency_ms,
        k,
        coalesce(latency_ms, 0) as latency_ms_filled,
        coalesce(k, 'N/A') as k_filled
    from flattened
    where log_ts >= to_date('2025-06-02')
      and k is not null
      and service in ('search', 'auth', 'checkout')
),
base as (
    select
        service,
        host,
        log_ts,
        latency_ms,
        latency_ms_filled,
        to_date(log_ts) as log_ts_date,
        coalesce(k, 'NONE') as k_clean
    from filtered
    where host in ('h3', 'h2', 'h4')
),
stats as (
    select
        date_trunc('week', log_ts_date) as week_start,
        date_trunc('year', log_ts_date) as year_start,
        (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min,
        sum(latency_ms) filter (where host = 'h4') as h4_total_latency_ms
    from base
    group by date_trunc('week', log_ts_date), date_trunc('year', log_ts_date)
)
select
    week_start,
    year_start,
    duration_min,
    h4_total_latency_ms
from stats
