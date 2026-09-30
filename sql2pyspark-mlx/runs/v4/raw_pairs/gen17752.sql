WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        status_code,
        payload_json,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
    FROM app_logs
    WHERE service = 'billing'
)
-- Grouped metrics
SELECT
    CASE WHEN retry_count >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS retry_count_tier,
    ROUND(AVG(has_fault), 2) AS avg_has_fault,
    COUNT(*) AS cnt,
    GROUPING(CASE WHEN retry_count >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM unpacked
GROUP BY ROLLUP (CASE WHEN retry_count >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY avg_has_fault
