select
    id,
    log_ts,
    level,
    latency_ms,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    context['trace'] as trace,
    get_json_object(payload_json, '$.duration') as duration_text,
    context['region'] as region
from app_logs
