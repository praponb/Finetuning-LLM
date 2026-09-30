SELECT
    id,
    host,
    context,
    COALESCE(context['region'], 'unknown') AS region_or_default,
    ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
    context['region'] AS region,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
FROM app_logs;
