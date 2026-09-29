WITH flattened AS (
    SELECT
        id,
        log_ts,
        level,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
valid AS (
    SELECT
        id,
        log_ts,
        level,
        k,
        value,
        HOUR(log_ts) AS log_ts_hour
    FROM flattened
    WHERE k IS NOT NULL
      AND log_ts >= '2024-06-01'
),
with_windows AS (
    SELECT
        *,
        SUM(log_ts_hour) OVER w_grp AS k_sum_log_ts_hour,
        COUNT(log_ts_hour) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS log_ts_hour_count_2h,
        PERCENT_RANK() OVER (PARTITION BY k, level ORDER BY log_ts_hour DESC) AS log_ts_hour_percent_rank
    FROM valid
    WINDOW w_grp AS (PARTITION BY k, level)
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY k_sum_log_ts_hour DESC) AS k_sum_log_ts_hour_dense_rank,
        COUNT(k_sum_log_ts_hour) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS k_sum_log_ts_hour_count_1h,
        STDDEV_SAMP(k_sum_log_ts_hour) OVER (PARTITION BY k) AS k_stddev_samp_k_sum_log_ts_hour,
        PERCENT_RANK() OVER w_running AS log_ts_hour_count_2h_percent_rank,
        MIN(log_ts_hour) OVER (PARTITION BY k, level) AS k_min_log_ts_hour
    FROM with_windows
    WINDOW w_running AS (PARTITION BY k ORDER BY log_ts_hour_count_2h DESC NULLS LAST)
),
cleaned AS (
    SELECT
        id,
        k,
        k_sum_log_ts_hour,
        log_ts_hour_percent_rank,
        k_sum_log_ts_hour_dense_rank,
        k_sum_log_ts_hour_count_1h,
        log_ts_hour_count_2h_percent_rank,
        k_min_log_ts_hour,
        CAST(log_ts_hour_count_2h_percent_rank AS INT) AS log_ts_hour_count_2h_percent_rank_int,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM scored
    WHERE level NOT IN ('DEBUG', 'WARN', 'INFO')
)
SELECT
    CASE WHEN k_min_log_ts_hour >= 88 THEN 'LARGE' ELSE 'SMALL' END AS k_min_log_ts_hour_tier,
    MAX(k_sum_log_ts_hour_dense_rank) AS max_k_sum_log_ts_hour_dense_rank,
    ROUND(SUM(k_sum_log_ts_hour) / NULLIF(COUNT(CASE WHEN k = 'region' THEN 1 END), 0), 2) AS k_sum_log_ts_hour_per_region,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY CASE WHEN k_min_log_ts_hour >= 88 THEN 'LARGE' ELSE 'SMALL' END
