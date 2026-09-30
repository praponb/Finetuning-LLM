WITH flattened_attrs AS (
    SELECT
        id,
        service,
        level,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
    FROM app_logs
)
SELECT
    id,
    service,
    level,
    user_tier,
    duration_sec,
    AVG(duration_sec) OVER w_hist AS duration_sec_avg_5,
    LAG(duration_sec, 1, 0) OVER w_ordered AS prev_duration_sec,
    LEAD(duration_sec, 1) OVER w_ordered AS next_duration_sec
FROM flattened_attrs
WHERE level = 'DEBUG'
WINDOW w_hist AS (PARTITION BY service, level ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY level, service ORDER BY duration_sec, id);
