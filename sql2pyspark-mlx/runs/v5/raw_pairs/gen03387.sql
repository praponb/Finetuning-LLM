with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value,
        date_trunc('YEAR', log_ts) as log_ts_period
    from flattened
    where log_ts >= date '2025-06-01'
),
with_windows as (
    -- Ranking and gap features
    select
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value,
        log_ts_period,
        percent_rank() over (partition by host order by latency_ms desc nulls first) as latency_ms_percent_rank,
        round(latency_ms / sum(latency_ms) over (partition by attr_key), 4) as latency_ms_share,
        round(latency_ms / sum(latency_ms) over (partition by host), 4) as latency_ms_share_2,
        round(latency_ms / sum(latency_ms) over (partition by attr_key), 4) as latency_ms_share_3,
        avg(latency_ms) over (partition by attr_key, host) as attr_key_avg_latency_ms
    from base
    where log_ts_period >= '2025-06-28'
),
with_windows_app_logs as (
    -- Window metrics over several different partitions
    select
        host,
        log_ts,
        attr_key,
        attr_value,
        log_ts_period,
        latency_ms_percent_rank,
        latency_ms_share_2,
        count(attr_key_avg_latency_ms) over (partition by attr_key) as attr_key_count_attr_key_avg_latency_ms,
        count(latency_ms_share_3) over (partition by attr_key order by cast(log_ts as bigint) range between 7200 preceding and current row) as latency_ms_share_3_count_2h
    from with_windows
)
-- Window metrics over several different partitions
select
    host,
    log_ts,
    attr_value,
    log_ts_period,
    latency_ms_percent_rank,
    latency_ms_share_2,
    attr_key_count_attr_key_avg_latency_ms,
    avg(latency_ms_share_3_count_2h) over (order by cast(log_ts_period as bigint) range between 2592000 preceding and current row) as latency_ms_share_3_count_2h_avg_30d,
    avg(latency_ms_share_3_count_2h) over (partition by attr_key order by cast(log_ts as bigint) range between 3600 preceding and current row) as latency_ms_share_3_count_2h_avg_1h,
    max(latency_ms_share_2) over w_recent as latency_ms_share_2_max_30m
from with_windows_app_logs
window w_recent as (partition by host, attr_key order by cast(log_ts as bigint) range between 1800 preceding and current row)
