with lines as (
    select
        id,
        service,
        host,
        level,
        latency_ms,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) li as k, attr_value
)
select
    level,
    host,
    round(avg(latency_ms), 2) as avg_latency_ms,
    count(*) as event_count,
    min(latency_ms) as min_latency_ms,
    count(*) as row_count
from lines
group by level, host
