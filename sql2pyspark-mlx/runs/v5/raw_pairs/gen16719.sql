WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        SIZE(context) AS num_context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        context['version'] AS version
    FROM app_logs
)
SELECT
    id,
    service,
    version,
    LAG(duration_sec, 2) OVER w_hist AS prev_duration_sec,
    SUM(num_context) OVER (ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_num_context,
    MIN(latency_ms) OVER (PARTITION BY service) AS service_min_latency_ms
FROM flattened_attrs
WINDOW w_hist AS (PARTITION BY level ORDER BY latency_ms ASC, id ASC);
