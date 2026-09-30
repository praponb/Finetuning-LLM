with flattened_attrs as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        payload_json,
        context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
        get_json_object(payload_json, '$.duration') as duration_text,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        array_contains(map_keys(context), 'trace') as has_trace
    from app_logs
)
select
    id,
    service,
    host,
    log_ts,
    latency_ms,
    payload_json,
    context,
    tier,
    duration_text,
    duration_sec,
    has_trace,
    lead(latency_ms, 2) over (partition by host order by duration_sec, id) next_latency_ms,
    ntile(5) over (order by latency_ms, id) latency_ms_bucket
from flattened_attrs
