with lines as (
    select
        id,
        service,
        latency_ms,
        k,
        attr_value
    from app_logs
    lateral view explode(context) x as k, attr_value
)
select
    id,
    service,
    latency_ms,
    k
from lines
order by latency_ms nulls first;
