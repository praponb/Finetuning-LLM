with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    from app_logs
    lateral view outer explode(context) li as key, v
),
filtered as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), log_ts) as days_since_log_ts
    from flattened
    where log_ts >= date '2025-06-01'
      and level in ('ERROR', 'DEBUG', 'WARN')
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        max(latency_ms) over (partition by host) as host_max_latency_ms,
        cume_dist() over (partition by service order by days_since_log_ts nulls first) as days_since_log_ts_cume_dist,
        count(days_since_log_ts) over (partition by key order by cast(log_ts as bigint) range between 7200 preceding and current row) as days_since_log_ts_count_2h
    from filtered
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v,
        days_since_log_ts,
        host_max_latency_ms,
        days_since_log_ts_cume_dist,
        days_since_log_ts_count_2h,
        dense_rank() over (partition by key, host order by days_since_log_ts nulls first) as days_since_log_ts_dense_rank,
        count(days_since_log_ts) over w_running as host_count_days_since_log_ts,
        round(days_since_log_ts_count_2h / sum(days_since_log_ts_count_2h) over (partition by key, host), 4) as days_since_log_ts_count_2h_share
    from with_windows
    window w_running as (partition by host)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(days_since_log_ts) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) as days_since_log_ts_max_30m,
        avg(days_since_log_ts_count_2h_share) over (partition by key) as key_avg_days_since_log_ts_count_2h_share
    from metrics
),
kept_keys as (
    -- Set difference on keys
    select level, service
    from scored
    except distinct
    select level, service
    from app_logs
    where service != 'auth'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join kept_keys k on s.level = k.level and s.service = k.service
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when days_since_log_ts_count_2h >= 16 then 'High' when days_since_log_ts_count_2h >= 3 then 'Medium' else 'Low' end as days_since_log_ts_count_2h_band
    from kept_rows
    where log_ts > to_date('2025-06-01')
)
-- Running and moving aggregates
select
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    key,
    v,
    days_since_log_ts,
    host_max_latency_ms,
    days_since_log_ts_cume_dist,
    days_since_log_ts_count_2h,
    days_since_log_ts_dense_rank,
    host_count_days_since_log_ts,
    days_since_log_ts_count_2h_share,
    days_since_log_ts_max_30m,
    key_avg_days_since_log_ts_count_2h_share,
    days_since_log_ts_count_2h_band,
    sum(days_since_log_ts_cume_dist) over w_running as days_since_log_ts_cume_dist_sum_30m,
    percent_rank() over w_running_2 as days_since_log_ts_max_30m_percent_rank,
    avg(latency_ms) over w_running_3 as service_avg_latency_ms
from cleaned
window w_running as (order by cast(log_ts as bigint) range between 1800 preceding and current row), w_running_2 as (partition by service order by days_since_log_ts_max_30m nulls first), w_running_3 as (partition by service);
