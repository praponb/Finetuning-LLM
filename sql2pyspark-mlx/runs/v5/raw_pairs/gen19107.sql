WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
)
SELECT
    host,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(CASE WHEN level = 'INFO' THEN 1 END) AS info_count,
    SUM(CASE WHEN attr_key = 'trace' THEN status_code ELSE 0 END) AS trace_status_code,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    APPROX_PERCENTILE(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles_2,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list
FROM lines
GROUP BY ROLLUP (host, DATE_TRUNC('HOUR', log_ts))
ORDER BY trace_status_code
