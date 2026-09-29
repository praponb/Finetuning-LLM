WITH exploded AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
flattened_attrs AS (
    SELECT
        level,
        k,
        v,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE level IN ('ERROR', 'DEBUG', 'INFO')
),
with_windows AS (
    SELECT
        k,
        v,
        is_flagged,
        AVG(status_code_high) OVER (PARTITION BY level) AS level_avg_status_code_high,
        CUME_DIST() OVER (PARTITION BY level ORDER BY status_code_high NULLS FIRST) AS status_code_high_cume_dist
    FROM flattened_attrs
),
scored AS (
    SELECT
        level_avg_status_code_high,
        status_code_high_cume_dist,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code_high_cume_dist NULLS FIRST) AS status_code_high_cume_dist_dense_rank,
        COUNT(is_flagged) OVER w_grp AS k_count_is_flagged,
        SUM(level_avg_status_code_high) OVER w_grp AS k_sum_level_avg_status_code_high,
        DENSE_RANK() OVER w_recent AS status_code_high_cume_dist_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_dense_rank
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY k), w_recent AS (PARTITION BY k ORDER BY status_code_high_cume_dist DESC NULLS FIRST)
)
SELECT
    CASE WHEN status_code_high_cume_dist_dense_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_cume_dist_dense_rank_2_tier,
    CASE WHEN k_count_is_flagged >= 16 THEN 'PREMIUM' ELSE 'STANDARD' END AS k_count_is_flagged_tier,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2,
    MAX(status_code_high_cume_dist_dense_rank) AS max_status_code_high_cume_dist_dense_rank
FROM scored
GROUP BY status_code_high_cume_dist_dense_rank_2_tier, k_count_is_flagged_tier
ORDER BY cnt_2 NULLS LAST;
