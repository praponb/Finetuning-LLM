WITH exploded AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
cleaned AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        k,
        v,
        CEIL(status_code) AS status_code_whole
    FROM exploded
    WHERE k IS NOT NULL
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code_whole DESC) AS status_code_whole_dense_rank,
        MAX(status_code_whole) OVER (PARTITION BY level) AS level_max_status_code_whole,
        RANK() OVER w_recent AS status_code_rank
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY k ORDER BY status_code DESC)
),
with_windows AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        k,
        v,
        status_code_whole,
        status_code_whole_dense_rank,
        level_max_status_code_whole,
        status_code_rank,
        ROUND(status_code_rank / SUM(status_code_rank) OVER (PARTITION BY k), 4) AS status_code_rank_share,
        SUM(latency_ms) OVER (PARTITION BY k) AS k_sum_latency_ms,
        MAX(level_max_status_code_whole) OVER (PARTITION BY level) AS level_max_level_max_status_code_whole,
        MIN(status_code_whole) OVER (PARTITION BY level) AS level_min_status_code_whole,
        STDDEV_SAMP(level_max_status_code_whole) OVER (PARTITION BY k, level) AS k_stddev_samp_level_max_status_code_whole
    FROM scored
),
with_windows_app_logs AS (
    SELECT
        *,
        MAX(k_sum_latency_ms) OVER w_ordered AS k_max_k_sum_latency_ms,
        RANK() OVER (PARTITION BY level, k ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank,
        MIN(level_min_status_code_whole) OVER w_ordered AS k_min_level_min_status_code_whole
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY k)
),
remaining_keys AS (
    SELECT id, level
    FROM with_windows_app_logs
    INTERSECT
    SELECT id, level
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2025-07-20'
),
kept_rows AS (
    SELECT s.*
    FROM with_windows_app_logs s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
),
summary AS (
    SELECT
        CASE WHEN level_min_status_code_whole >= 75 THEN 'LARGE' ELSE 'SMALL' END AS level_min_status_code_whole_tier,
        COUNT(*) AS cnt,
        COUNT(DISTINCT level) AS distinct_levels,
        SORT_ARRAY(COLLECT_SET(k)) AS k_list,
        BIT_OR(status_code) AS status_code_bit_or
    FROM kept_rows
    GROUP BY CASE WHEN level_min_status_code_whole >= 75 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    level_min_status_code_whole_tier,
    cnt,
    distinct_levels,
    k_list
FROM summary
ORDER BY cnt DESC, level_min_status_code_whole_tier;
