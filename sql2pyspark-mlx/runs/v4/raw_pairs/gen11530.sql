with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) li as k, attr_value
)
-- Window metrics over several different partitions
select
    *,
    max(latency_ms) over (partition by k order by cast(log_ts as bigint) range between 1800 preceding and current row) as latency_ms_max_30m,
    round(status_code / sum(status_code) over (partition by host), 4) as status_code_share
from exploded
