WITH lines AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
flattened_attrs AS (
    SELECT
        id,
        level,
        k,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM lines
),
scored AS (
    SELECT
        id,
        level,
        k,
        value,
        SUM(status_code_high) OVER (ORDER BY has_fault, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_high,
        LEAD(status_code_high, 1) OVER (PARTITION BY level ORDER BY status_code_high, id, k) AS next_status_code_high,
        NTILE(10) OVER (PARTITION BY k ORDER BY has_fault, id, k) AS has_fault_bucket,
        AVG(has_fault) OVER (PARTITION BY level ORDER BY status_code_high, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS has_fault_avg_6,
        LAST_VALUE(k) IGNORE NULLS OVER (PARTITION BY level ORDER BY has_fault, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k
    FROM flattened_attrs
),
metrics AS (
    SELECT
        id,
        level,
        k,
        value,
        next_status_code_high,
        has_fault_bucket,
        has_fault_avg_6,
        AVG(has_fault_avg_6) OVER (PARTITION BY k ORDER BY has_fault_avg_6, id, k ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS has_fault_avg_6_avg_5,
        NTILE(10) OVER (PARTITION BY level ORDER BY running_status_code_high, id, k) AS running_status_code_high_bucket,
        LAST_VALUE(has_fault_bucket, TRUE) OVER (PARTITION BY k, level ORDER BY has_fault_bucket, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_has_fault_bucket,
        LAST_VALUE(has_fault_avg_6, TRUE) OVER (PARTITION BY k ORDER BY next_status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_has_fault_avg_6,
        MAX(has_fault_avg_6) OVER (PARTITION BY level) AS level_max_has_fault_avg_6
    FROM scored
    WHERE running_status_code_high IS NOT NULL
),
scored_app_logs AS (
    SELECT
        id,
        k,
        next_status_code_high,
        running_status_code_high_bucket,
        level_max_has_fault_avg_6,
        COUNT(level_max_has_fault_avg_6) OVER (PARTITION BY level ORDER BY level_max_has_fault_avg_6, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_level_max_has_fault_avg_6,
        SUM(level_max_has_fault_avg_6) OVER (PARTITION BY level ORDER BY level_max_has_fault_avg_6, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_level_max_has_fault_avg_6,
        AVG(level_max_has_fault_avg_6) OVER (PARTITION BY level ORDER BY level_max_has_fault_avg_6, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_level_max_has_fault_avg_6,
        MIN(level_max_has_fault_avg_6) OVER (PARTITION BY level ORDER BY level_max_has_fault_avg_6, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_level_max_has_fault_avg_6
    FROM metrics
)
SELECT
    k,
    CASE WHEN level_max_has_fault_avg_6 >= 1 THEN 'HIGH' ELSE 'LOW' END AS level_max_has_fault_avg_6_tier,
    SUM(level_max_has_fault_avg_6) AS total_level_max_has_fault_avg_6,
    COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
    SIZE(COLLECT_SET(k)) AS num_ks,
    COUNT(CASE WHEN k = 'trace' THEN 1 END) AS trace_count,
    SIZE(COLLECT_SET(k)) AS num_ks_2,
    COUNT(*) AS cnt
FROM scored_app_logs
GROUP BY k, CASE WHEN level_max_has_fault_avg_6 >= 1 THEN 'HIGH' ELSE 'LOW' END
ORDER BY k NULLS FIRST
