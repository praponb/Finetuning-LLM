with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
),
metrics as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by attr_key order by latency_ms desc) as latency_ms_percent_rank,
        sum(latency_ms) over (order by cast(log_ts as bigint) asc range between 3600 preceding and current row) as latency_ms_sum_1h,
        count(latency_ms) over (order by cast(log_ts as bigint) asc range between 3600 preceding and current row) as latency_ms_count_1h
    from flattened
),
metrics_app_logs as (
    -- Window metrics over several different partitions
    select
        *,
        avg(latency_ms) over w_ordered as latency_ms_avg_30m,
        dense_rank() over (order by latency_ms_count_1h desc nulls last) as latency_ms_count_1h_dense_rank,
        max(latency_ms) over w_running as latency_ms_max_2h
    from metrics
    window w_ordered as (order by cast(log_ts as bigint) asc range between 1800 preceding and current row), w_running as (partition by service order by cast(log_ts as bigint) asc range between 7200 preceding and current row)
),
kept_keys as (
    -- Set intersection on keys
    select service, id
    from metrics_app_logs
    intersect all
    select service, id
    from app_logs
    where status_code <= 317
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics_app_logs s
    inner join kept_keys k on s.service = k.service and s.id = k.id
),
filtered as (
    -- Basic cleansing and defaults
    select *, datediff(current_date(), log_ts) as days_since_log_ts
    from reconciled
    where log_ts >= timestamp '2025-06-01 14:00:00'
      and attr_key is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        round(latency_ms / sum(latency_ms) over (partition by service), 4) as latency_ms_share,
        avg(days_since_log_ts) over (partition by attr_key) as attr_key_avg_days_since_log_ts
    from filtered
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        service,
        level,
        v,
        latency_ms_percent_rank,
        latency_ms_sum_1h,
        latency_ms_count_1h,
        latency_ms_avg_30m,
        latency_ms_count_1h_dense_rank,
        latency_ms_max_2h,
        latency_ms_share,
        attr_key_avg_days_since_log_ts,
        round(latency_ms / latency_ms_count_1h_dense_rank, 2) as latency_ms_per_latency_ms_count_1h_dense_rank
    from enriched
    where log_ts >= timestamp '2025-06-01 10:00:00'
)
-- Grouped metrics
select
    service,
    case when latency_ms_max_2h >= 2072 then 'HIGH' else 'LOW' end as latency_ms_max_2h_tier,
    case when latency_ms_avg_30m >= 2004 then 'HIGH' else 'LOW' end as latency_ms_avg_30m_tier,
    avg(latency_ms_per_latency_ms_count_1h_dense_rank) filter (where level = 'WARN') as warn_avg_latency_ms_per_latency_ms_count_1h_dense_rank,
    size(collect_set(level)) as num_levels,
    round(avg(latency_ms_percent_rank), 2) as avg_latency_ms_percent_rank,
    sum(latency_ms_avg_30m) as total_latency_ms_avg_30m
from valid
group by grouping sets ((service, case when latency_ms_max_2h >= 2072 then 'HIGH' else 'LOW' end, case when latency_ms_avg_30m >= 2004 then 'HIGH' else 'LOW' end), (service, case when latency_ms_max_2h >= 2072 then 'HIGH' else 'LOW' end))
order by latency_ms_avg_30m_tier asc nulls first;
