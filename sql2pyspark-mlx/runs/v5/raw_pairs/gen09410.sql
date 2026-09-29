SELECT
    id,
    service,
    level,
    status_code,
    context,
    COALESCE(context['version'], 'none') AS version_or_default,
    context['trace'] AS trace,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    COALESCE(context['region'], 'none') AS region_or_default,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
FROM app_logs;
