with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        service,
        log_ts,
        latency_ms,
        payload_json,
        array_contains(map_keys(context), 'version') as has_version,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
    from app_logs
    where service in ('auth', 'checkout')
)
-- Running and moving aggregates
select
    id,
    latency_ms,
    avg(retry_count) over w_recent last3_avg_retry_count,
    count(retry_count) over w_recent last3_count_retry_count,
    max(retry_count) over w_recent last3_max_retry_count,
    min(retry_count) over w_recent last3_min_retry_count,
    avg(latency_ms) over (partition by service order by cast(log_ts as bigint) range between 7200 preceding and current row) latency_ms_avg_2h
from flattened_attrs
window w_recent as (partition by service order by latency_ms, id rows between 2 preceding and current row);
