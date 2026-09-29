WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        level,
        status_code,
        attr_key,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY level ORDER BY has_fault DESC, id, attr_key) AS has_fault_bucket,
        DENSE_RANK() OVER (ORDER BY status_code) AS status_code_dense_rank
    FROM parsed
    WHERE attr_key NOT IN ('region', 'trace')
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(has_fault_bucket, TRUE) OVER (ORDER BY has_fault, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_has_fault_bucket,
        AVG(has_fault) OVER (ORDER BY status_code_high, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS has_fault_avg_3
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_dense_rank_tier,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    AVG(first_known_has_fault_bucket) AS avg_first_known_has_fault_bucket,
    COUNT_IF(level = 'INFO') AS info_events
FROM with_windows_app_logs
GROUP BY CUBE (CASE WHEN status_code_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING avg_first_known_has_fault_bucket > 0
ORDER BY avg_first_known_has_fault_bucket
