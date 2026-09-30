WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
scored AS (
    SELECT
        *,
        COUNT(latency_ms) OVER w_ordered AS host_count_latency_ms,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist,
        AVG(latency_ms) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_avg_30m
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY host)
),
filtered AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        host_count_latency_ms,
        latency_ms_cume_dist,
        latency_ms_avg_30m,
        LENGTH(level) AS level_length
    FROM scored
    WHERE log_ts > TIMESTAMP '2025-06-01 14:00:00'
),
filtered_app_logs AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM filtered
    WHERE log_ts >= DATE '2025-06-01'
),
cleaned AS (
    SELECT *, GREATEST(status_code, days_since_log_ts) AS greatest_status_code_days_since_log_ts
    FROM filtered_app_logs
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 10:00:00')
)
SELECT
    service,
    latency_ms,
    status_code,
    latency_ms_cume_dist,
    latency_ms_avg_30m,
    level_length,
    days_since_log_ts,
    CASE WHEN service RLIKE '^(billing|auth)$' THEN 'CORE' WHEN service RLIKE '^(search|checkout)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS service_zone
FROM cleaned
ORDER BY status_code;
