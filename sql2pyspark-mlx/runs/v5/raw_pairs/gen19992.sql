SELECT
    id,
    log_ts,
    payload_json,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    COALESCE(context['version'], 'none') AS version_or_default
FROM app_logs
WHERE log_ts < '2025-06-29';
