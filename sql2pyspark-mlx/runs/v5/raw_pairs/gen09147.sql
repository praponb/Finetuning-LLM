WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
)
-- Multi-level aggregation
SELECT
    key,
    CASE WHEN status_code >= 455 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    SUM(CASE WHEN level = 'INFO' THEN status_code ELSE 0 END) AS info_status_code
FROM lines
GROUP BY key, CASE WHEN status_code >= 455 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY key DESC NULLS LAST;
