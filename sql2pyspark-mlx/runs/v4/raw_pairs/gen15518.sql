WITH lines AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
)
SELECT
    attr_key,
    CASE WHEN latency_ms >= 1879 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2,
    GROUPING_ID() AS grouping_level
FROM lines
GROUP BY GROUPING SETS ((attr_key, CASE WHEN latency_ms >= 1879 THEN 'LARGE' ELSE 'SMALL' END), (attr_key))
ORDER BY attr_key DESC NULLS FIRST
