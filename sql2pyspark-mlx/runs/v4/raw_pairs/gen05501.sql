with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        k,
        v
    from app_logs
    lateral view outer explode(context) t as k, v
)
-- Aggregate with conditional metrics
select
    k,
    date_trunc('hour', log_ts) as hour_window,
    count(*) filter (where k = 'version') as version_rows,
    max(log_ts) as last_seen,
    sort_array(collect_set(k)) as k_list,
    count(distinct k) as distinct_ks,
    count(distinct k) as distinct_ks_2
from exploded
group by grouping sets ((k, date_trunc('hour', log_ts)), (k))
having version_rows < 1
