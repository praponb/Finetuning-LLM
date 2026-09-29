select
    id,
    service,
    log_ts,
    latency_ms,
    payload_json,
    coalesce(context['version'], 'direct') as version_or_default,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier
from app_logs;
