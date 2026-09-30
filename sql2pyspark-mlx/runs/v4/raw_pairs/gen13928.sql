WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
unpacked AS (
    SELECT
        id,
        service,
        level,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM flattened
    WHERE service NOT IN ('billing', 'checkout', 'auth')
),
with_windows AS (
    SELECT
        id,
        service,
        level,
        status_code_high,
        has_fault,
        RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC) AS status_code_high_rank,
        COUNT(has_fault) OVER (PARTITION BY service) AS service_count_has_fault,
        RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_rank_2
    FROM unpacked
),
stats AS (
    SELECT
        CASE WHEN status_code_high_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_rank_2_tier,
        service,
        SUM(status_code_high) AS total_status_code_high,
        ROUND(AVG(has_fault), 2) AS avg_has_fault,
        AVG(status_code_high) FILTER (WHERE level = 'INFO') AS info_avg_status_code_high,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN status_code_high_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY AVG(status_code_high) FILTER (WHERE level = 'INFO') DESC) AS info_avg_status_code_high_dense_rank
    FROM with_windows
    GROUP BY CASE WHEN status_code_high_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, service
)
SELECT
    status_code_high_rank_2_tier,
    service,
    avg_has_fault,
    info_avg_status_code_high
FROM stats
ORDER BY status_code_high_rank_2_tier DESC NULLS LAST;
