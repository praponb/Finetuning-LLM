WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        v,
        CAST(latency_ms AS DOUBLE) AS latency_ms_num,
        INITCAP(TRIM(key)) AS key_title
    FROM exploded
    WHERE key IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        level,
        latency_ms,
        status_code,
        v,
        latency_ms_num,
        key_title,
        MAX(latency_ms_num) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_num_max_15m,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms DESC) AS latency_ms_dense_rank,
        SUM(status_code) OVER (PARTITION BY service) AS service_sum_status_code,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_num DESC NULLS FIRST) AS latency_ms_num_cume_dist,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_sum_15m
    FROM cleaned
    WHERE level = 'WARN'
),
matched_keys AS (
    -- Set difference on keys
    SELECT level
    FROM scored
    EXCEPT DISTINCT
    SELECT level
    FROM app_logs
    WHERE latency_ms <= 2205
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.level = k.level
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(latency_ms_num_max_15m, 2) AS latency_ms_num_max_15m_rounded
    FROM reconciled
    WHERE status_code > 437
)
-- Multi-level aggregation
SELECT
    level,
    CASE WHEN service_sum_status_code >= 1756 THEN 'PREMIUM' ELSE 'STANDARD' END AS service_sum_status_code_tier,
    CASE WHEN status_code >= 454 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    AVG(status_code_sum_15m) AS avg_status_code_sum_15m,
    COUNT(*) FILTER (WHERE level = 'INFO') AS info_rows
FROM base
GROUP BY level, CASE WHEN service_sum_status_code >= 1756 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_code >= 454 THEN 'HIGH' ELSE 'LOW' END;
