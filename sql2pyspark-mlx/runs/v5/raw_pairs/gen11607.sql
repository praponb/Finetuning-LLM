WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        payload_json,
        ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SIZE(context) AS num_context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
-- Ranking and gap features
SELECT
    id,
    service,
    status_code,
    has_region,
    num_context,
    user_id,
    NTILE(3) OVER (PARTITION BY service ORDER BY retry_count DESC, id) AS retry_count_bucket,
    CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist,
    AVG(latency_ms) OVER (ORDER BY latency_ms, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS latency_ms_avg_6,
    NTILE(4) OVER (ORDER BY num_context DESC, id) AS num_context_bucket
FROM parsed
