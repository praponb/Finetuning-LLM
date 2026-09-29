select
    id,
    log_ts,
    level,
    latency_ms,
    status_code,
    context,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
    coalesce(context['region'], 'unknown') as region_or_default,
    get_json_object(payload_json, '$.duration') as duration_text,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
from app_logs
