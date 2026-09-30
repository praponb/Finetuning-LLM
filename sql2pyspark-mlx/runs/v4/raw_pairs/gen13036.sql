select
    id,
    log_ts,
    latency_ms,
    status_code,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    coalesce(context['trace'], 'unknown') as trace_or_default
from app_logs
where latency_ms < 2050
