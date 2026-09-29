with flattened as (
    select
        id,
        service,
        host,
        latency_ms,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
)
select
    id,
    latency_ms,
    attr_key,
    round(latency_ms / sum(latency_ms) over (partition by host), 4) latency_ms_share,
    min(latency_ms) over (partition by service) service_min_latency_ms,
    dense_rank() over w_recent latency_ms_dense_rank,
    round(latency_ms / sum(latency_ms) over (partition by service), 4) latency_ms_share_2
from flattened
window w_recent as (partition by host, service order by latency_ms)
