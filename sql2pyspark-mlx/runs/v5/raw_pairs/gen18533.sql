with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        latency_ms,
        attr_key,
        value
    from app_logs
    lateral view explode(context) x as attr_key, value
)
-- Ranking and gap features
select
    id,
    log_ts,
    attr_key,
    lead(latency_ms) over w_recent next_latency_ms,
    row_number() over w_recent row_seq,
    rank() over (partition by attr_key, host order by latency_ms asc nulls first) latency_ms_rank
from flattened
window w_recent as (partition by attr_key order by latency_ms asc, id asc, attr_key asc)
