SELECT
    id,
    host,
    log_ts,
    latency_ms,
    SIZE(context) AS num_context,
    ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region
FROM app_logs;
