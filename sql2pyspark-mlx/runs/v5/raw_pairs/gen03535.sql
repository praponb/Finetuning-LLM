WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
flattened_attrs AS (
    SELECT
        host,
        log_ts,
        level,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM lines
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level, host ORDER BY is_flagged ASC NULLS LAST) AS is_flagged_dense_rank,
        SUM(is_flagged) OVER (PARTITION BY level) AS level_sum_is_flagged,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY level, host) AS level_stddev_samp_status_code_high
    FROM flattened_attrs
),
remaining_keys AS (
    SELECT host, level
    FROM with_windows
    INTERSECT
    SELECT host, level
    FROM app_logs
    WHERE latency_ms < 1676
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.host = k.host AND s.level = k.level
),
scored AS (
    SELECT
        *,
        AVG(level_sum_is_flagged) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS level_sum_is_flagged_avg_30m,
        AVG(is_flagged_dense_rank) OVER (PARTITION BY host, level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_flagged_dense_rank_avg_2h,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY level_stddev_samp_status_code_high DESC NULLS LAST) AS level_stddev_samp_status_code_high_percent_rank,
        RANK() OVER w_ordered AS level_sum_is_flagged_rank,
        RANK() OVER (PARTITION BY level, host ORDER BY status_code_high ASC NULLS FIRST) AS status_code_high_rank
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY level ORDER BY level_sum_is_flagged DESC)
),
with_windows_app_logs AS (
    SELECT
        log_ts,
        level,
        is_flagged,
        level_sum_is_flagged,
        level_sum_is_flagged_avg_30m,
        level_stddev_samp_status_code_high_percent_rank,
        status_code_high_rank,
        ROUND(level_sum_is_flagged_rank / SUM(level_sum_is_flagged_rank) OVER (PARTITION BY host, level), 4) AS level_sum_is_flagged_rank_share,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY is_flagged_dense_rank DESC) AS is_flagged_dense_rank_percent_rank,
        MAX(is_flagged_dense_rank) OVER w_ordered AS is_flagged_dense_rank_max_15m,
        RANK() OVER (PARTITION BY level ORDER BY level_sum_is_flagged_rank DESC NULLS FIRST) AS level_sum_is_flagged_rank_rank
    FROM scored
    WINDOW w_ordered AS (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    SUM(CASE WHEN level = 'INFO' THEN status_code_high_rank ELSE 0 END) AS info_status_code_high_rank,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_rows,
    PERCENTILE_APPROX(level_sum_is_flagged_avg_30m, 0.9) AS level_sum_is_flagged_avg_30m_p90,
    MAX(log_ts) AS last_log_ts
FROM with_windows_app_logs
GROUP BY DATE_TRUNC('HOUR', log_ts);
