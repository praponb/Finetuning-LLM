SELECT
    id,
    service,
    host,
    log_ts,
    level,
    status_code,
    context,
    context['version'] AS version,
    ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
FROM app_logs;
