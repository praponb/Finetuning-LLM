SELECT
    id,
    service,
    host,
    log_ts,
    status_code,
    payload_json,
    context,
    CAST((status_code & 4) != 0 AS INT) AS is_flagged,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier
FROM app_logs
WHERE status_code < 356;
