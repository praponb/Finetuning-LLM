WITH exploded AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
),
unpacked AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE host IN ('h4', 'h1')
),
filtered AS (
    SELECT
        *,
        SUBSTRING(host, 1, 4) AS host_prefix,
        ROUND(has_fault * 0.9, 2) AS adjusted_has_fault
    FROM unpacked
    WHERE status_code_high <= 2
),
base AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code_high,
        has_fault,
        host_prefix,
        adjusted_has_fault * has_fault AS adjusted_has_fault_times_has_fault
    FROM filtered
    WHERE adjusted_has_fault >= 43
)
SELECT
    CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END AS has_fault_tier,
    level,
    host,
    MAX(status_code_high) AS max_status_code_high,
    COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows,
    SUM(CASE WHEN host = 'h4' THEN has_fault ELSE 0 END) AS h4_has_fault
FROM base
GROUP BY GROUPING SETS ((CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END, level, host), (CASE WHEN has_fault >= 1 THEN 'LARGE' ELSE 'SMALL' END, level), ())
HAVING max_status_code_high > 0;
