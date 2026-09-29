WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
)
-- Grouped metrics
SELECT
    k,
    APPROX_PERCENTILE(status_code, 0.9) AS status_code_p90,
    SUM(status_code) FILTER (WHERE k = 'version') AS version_total_status_code,
    COUNT_IF(host = 'h1') AS h1_events,
    COUNT(*) FILTER (WHERE level = 'INFO') AS info_rows,
    MAX(status_code) FILTER (WHERE host = 'h4') AS h4_max_status_code
FROM lines
GROUP BY k
HAVING COUNT(*) FILTER (WHERE level = 'INFO') > 1;
