WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(DISTINCT level) AS distinct_levels,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count
FROM lines
GROUP BY DATE_TRUNC('HOUR', log_ts)
