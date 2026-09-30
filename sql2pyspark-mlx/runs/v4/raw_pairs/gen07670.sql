with lines as (
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) x as k, v
),
enriched as (
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v,
        ntile(5) over (partition by level, k order by status_code desc, id, k) as status_code_bucket,
        avg(status_code) over (partition by k order by status_code, id, k rows between 3 preceding and current row) as status_code_avg_3
    from lines
),
scored as (
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v,
        status_code_bucket,
        status_code_avg_3,
        sum(status_code) over (partition by host, k order by status_code_avg_3, id, k rows between unbounded preceding and current row) as cumulative_status_code,
        max(status_code_avg_3) over (partition by k) as k_max_status_code_avg_3
    from enriched
)
select
    date_trunc('day', log_ts) as activity_day,
    host,
    max(log_ts) as last_log_ts,
    size(collect_set(level)) as num_levels,
    avg(status_code) filter (where level = 'ERROR') as error_avg_status_code,
    count(case when k = 'region' then 1 end) as region_count,
    max(log_ts) as last_seen,
    count(*) filter (where k = 'trace') as trace_rows,
    grouping(date_trunc('day', log_ts)) as grouping_level
from scored
group by grouping sets ((date_trunc('day', log_ts), host), (date_trunc('day', log_ts)), ());
