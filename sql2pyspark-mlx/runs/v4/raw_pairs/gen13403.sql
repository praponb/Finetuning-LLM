SELECT
    id,
    host,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    COALESCE(context['region'], 'none') AS region_or_default,
    COALESCE(context['version'], 'unknown') AS version_or_default
FROM app_logs
WHERE host IN ('h2', 'h1', 'h3')
