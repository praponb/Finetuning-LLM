with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        level,
        latency_ms,
        key,
        value
    from app_logs
    lateral view outer explode(context) t as key, value
)
-- Running and moving aggregates
select
    id,
    service,
    level,
    latency_ms,
    key,
    value,
    dense_rank() over (partition by service order by latency_ms desc) as latency_ms_dense_rank,
    round(latency_ms / sum(latency_ms) over (partition by service, level), 4) as latency_ms_share
from lines;
