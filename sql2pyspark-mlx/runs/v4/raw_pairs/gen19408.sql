WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        payload_json,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    CASE WHEN latency_ms >= 2484 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    service,
    PERCENTILE_APPROX(retry_count, 0.5) AS retry_count_p50,
    COUNT(CASE WHEN service = 'auth' THEN 1 END) AS auth_count,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count,
    GROUPING(CASE WHEN latency_ms >= 2484 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM unpacked
GROUP BY ROLLUP (CASE WHEN latency_ms >= 2484 THEN 'LARGE' ELSE 'SMALL' END, service)
HAVING PERCENTILE_APPROX(retry_count, 0.5) >= 0
ORDER BY latency_ms_tier DESC;
