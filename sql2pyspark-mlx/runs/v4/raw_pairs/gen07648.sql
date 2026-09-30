with lines as (
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
)
select
    level,
    count(distinct k) as distinct_ks,
    sort_array(collect_set(host)) as host_list,
    round(avg(latency_ms), 2) as avg_latency_ms,
    grouping_id() as grouping_id
from lines
group by cube (level)
