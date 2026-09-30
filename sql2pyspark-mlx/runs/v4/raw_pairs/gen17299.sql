select
    id,
    host,
    level,
    latency_ms,
    status_code,
    context,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    coalesce(context['region'], 'unknown') as region_or_default,
    context['version'] as version,
    context['trace'] as trace
from app_logs;
