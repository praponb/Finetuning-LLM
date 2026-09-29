WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    AVG(latency_ms) FILTER (WHERE k = 'region') AS region_avg_latency_ms,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    SUM(latency_ms) FILTER (WHERE k = 'trace') AS trace_total_latency_ms,
    MAX(log_ts) AS ended_at,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM lines
GROUP BY DATE_TRUNC('DAY', log_ts)
ORDER BY activity_day DESC NULLS LAST;
