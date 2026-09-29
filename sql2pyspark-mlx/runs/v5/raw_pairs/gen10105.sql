with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    from app_logs
    lateral view outer explode(context) x as k, v
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        round(status_code, 1) as status_code_rounded,
        coalesce(k, 'NONE') as k_clean
    from exploded
    where k is not null
      and status_code <= 401
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        v,
        case when status_code >= 400 then 'High' when status_code >= 275 then 'Medium' else 'Low' end as status_code_tier,
        coalesce(k, 'DIRECT') as k_clean_2
    from valid
    where log_ts > timestamp '2025-06-01 12:00:00'
      and hour(log_ts) between 11 and 18
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over w_hist as status_code_dense_rank,
        rank() over w_grp as status_code_rank
    from cleaned
    window w_hist as (order by status_code), w_grp as (partition by host order by status_code)
),
valid_app_logs as (
    -- Keep recent, valid rows
    select
        service,
        host,
        status_code,
        k,
        status_code_tier,
        status_code_dense_rank,
        status_code_rank,
        trim(host) as clean_host,
        coalesce(k, 'NONE') as k_clean
    from scored
    where log_ts >= date '2025-06-01'
      and k is not null
),
grouped as (
    -- Grouped metrics
    select
        case when status_code >= 396 then 'LARGE' else 'SMALL' end as status_code_tier,
        percentile_approx(status_code_dense_rank, 0.95) as status_code_dense_rank_p95,
        round(avg(status_code_dense_rank), 2) as avg_status_code_dense_rank
    from valid_app_logs
    group by case when status_code >= 396 then 'LARGE' else 'SMALL' end
)
select status_code_tier, status_code_dense_rank_p95
from grouped
order by status_code_dense_rank_p95
