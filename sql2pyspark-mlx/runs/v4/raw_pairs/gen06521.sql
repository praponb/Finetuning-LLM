WITH flattened AS (
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
    LATERAL VIEW EXPLODE(context) li AS k, value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id ASC, k ASC) AS seq_no,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_cume_dist,
        FIRST_VALUE(k, TRUE) OVER (PARTITION BY level, host ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k
    FROM flattened
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY k, level ORDER BY status_code_cume_dist ASC, id ASC, k ASC) AS seq_no_2,
        LEAD(status_code_cume_dist, 2) OVER (PARTITION BY host ORDER BY seq_no ASC, id ASC, k ASC) AS next_status_code_cume_dist
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        seq_no,
        status_code_cume_dist,
        first_known_k,
        seq_no_2,
        CAST(latency_ms AS STRING) AS latency_ms_str
    FROM scored
    WHERE log_ts >= DATE '2025-06-01'
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_cume_dist >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS status_code_cume_dist_tier,
    SUM(latency_ms) AS total_latency_ms,
    SIZE(COLLECT_SET(level)) AS num_levels,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM cleaned
GROUP BY CASE WHEN status_code_cume_dist >= 0.65 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY status_code_cume_dist_tier DESC
