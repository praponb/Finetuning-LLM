WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        level,
        k,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(status_code_high) OVER (PARTITION BY host) AS host_max_status_code_high
    FROM unpacked
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        level,
        k,
        flag_bit8,
        status_code_high,
        host_max_status_code_high,
        CONCAT_WS('-', k, level) AS k_level_key
    FROM metrics
    WHERE level IN ('ERROR', 'WARN')
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_tier,
        MAX(host_max_status_code_high) FILTER (WHERE host = 'h4') AS h4_max_host_max_status_code_high,
        COUNT(DISTINCT level) AS distinct_levels,
        ROUND(SUM(flag_bit8) / NULLIF(COUNT(CASE WHEN k = 'region' THEN 1 END), 0), 2) AS flag_bit8_per_region
    FROM cleaned
    GROUP BY CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING distinct_levels > 1
)
SELECT status_code_high_tier, h4_max_host_max_status_code_high
FROM grouped
ORDER BY h4_max_host_max_status_code_high DESC NULLS LAST;
