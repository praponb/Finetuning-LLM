with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) e as k, attr_value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        lower(service) as service_lower,
        case when level = 'WARN' then 1 else 0 end as is_warn
    from exploded
    where k is null
),
scored as (
    -- Running and moving aggregates
    select
        id,
        service,
        log_ts,
        status_code,
        is_warn,
        round(status_code / sum(status_code) over (partition by level), 4) as status_code_share
    from cleaned
    where service = 'billing'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        service,
        log_ts,
        status_code_share,
        cume_dist() over (partition by service order by status_code_share desc) as status_code_share_cume_dist
    from scored
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        count(status_code_share_cume_dist) over (order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_share_cume_dist_count_15m,
        avg(status_code_share) over (partition by service) as service_avg_status_code_share
    from with_windows
),
stats as (
    -- Multi-level aggregation
    select
        case when status_code_share_cume_dist >= 0.85 then 'PREMIUM' else 'STANDARD' end as status_code_share_cume_dist_tier,
        approx_percentile(status_code_share, 0.95) as status_code_share_p95,
        min(log_ts) as started_at,
        count(distinct id) as distinct_ids,
        count(*) filter (where service = 'auth') as auth_rows,
        max(log_ts) as last_log_ts
    from enriched
    group by 1
)
select
    status_code_share_cume_dist_tier,
    status_code_share_p95,
    distinct_ids,
    auth_rows,
    last_log_ts
from stats
order by last_log_ts desc;
