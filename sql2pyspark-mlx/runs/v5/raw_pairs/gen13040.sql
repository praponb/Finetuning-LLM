SELECT
    id,
    service,
    log_ts,
    level,
    latency_ms,
    context,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    context['version'] AS version,
    SIZE(context) AS num_context
FROM app_logs
WHERE service <> 'checkout';
