with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as key, attr_value
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value,
        least(latency_ms, status_code) least_latency_ms_status_code
    from flattened
    where key is not null
      and latency_ms = 1739
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        case when key in ('version', 'trace') then 'Priority' else 'Other' end key_group,
        coalesce(key, 'NONE') resolved_key
    from filtered
    where log_ts >= date '2025-06-01'
      and key is not null
),
base_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(key) key_upper,
        to_date(log_ts) log_ts_date
    from base
    where key is not null
),
base_app_logs_2 as (
    -- Keep recent, valid rows
    select
        *,
        round(status_code * 1.1, 2) adjusted_status_code,
        to_date(log_ts) log_ts_date_2
    from base_app_logs
    where log_ts > date '2025-06-01'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        min(latency_ms) over (partition by key) key_min_latency_ms,
        cume_dist() over (partition by key order by least_latency_ms_status_code desc nulls last) least_latency_ms_status_code_cume_dist,
        count(least_latency_ms_status_code) over (partition by key) key_count_least_latency_ms_status_code
    from base_app_logs_2
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        avg(least_latency_ms_status_code_cume_dist) over (order by cast(log_ts as bigint) range between 3600 preceding and current row) least_latency_ms_status_code_cume_dist_avg_1h,
        cume_dist() over (partition by key order by adjusted_status_code nulls last) adjusted_status_code_cume_dist,
        max(key_count_least_latency_ms_status_code) over (partition by key order by cast(log_ts as bigint) range between 1800 preceding and current row) key_count_least_latency_ms_status_code_max_30m,
        rank() over w_hist least_latency_ms_status_code_rank,
        corr(status_code, latency_ms) over (partition by key) corr_status_code_latency_ms
    from with_windows
    where log_ts < '2023-11-01'
    window w_hist as (partition by key order by least_latency_ms_status_code desc nulls last)
)
-- Multi-level aggregation
select
    key,
    date_trunc('month', log_ts_date_2) as activity_month,
    sort_array(collect_set(key)) as key_list,
    percentile_approx(least_latency_ms_status_code_rank, 0.5) as least_latency_ms_status_code_rank_p50,
    sum(case when key = 'version' then least_latency_ms_status_code_rank else 0 end) as version_least_latency_ms_status_code_rank,
    max(log_ts) as last_seen,
    min(log_ts) as first_seen
from enriched
group by key, activity_month
order by key nulls last
