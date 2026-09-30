WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        level,
        payload_json,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        context['region'] AS region
    FROM app_logs
    WHERE level = 'ERROR'
)
-- Aggregate with conditional metrics
SELECT
    level,
    service,
    SUM(status_code_high) FILTER (WHERE level = 'DEBUG') AS debug_total_status_code_high,
    AVG(status_code_high) AS avg_status_code_high,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high_2,
    PERCENTILE_APPROX(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles
FROM flattened_attrs
GROUP BY level, service
