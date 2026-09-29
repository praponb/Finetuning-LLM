SELECT
    id,
    level,
    status_code,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    COALESCE(context['trace'], 'unknown') AS trace_or_default,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    SHIFTRIGHT(status_code, 2) AS status_code_high
FROM app_logs;
