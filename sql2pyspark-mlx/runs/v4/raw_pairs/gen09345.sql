WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
scored AS (
    SELECT
        *,
        RANK() OVER (ORDER BY status_code DESC) AS status_code_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        SUM(status_code) OVER (PARTITION BY service, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key, service), 4) AS status_code_share_2
    FROM lines
),
filtered AS (
    SELECT
        *,
        CASE WHEN key IN ('trace', 'region') THEN 'Core' ELSE 'Other' END AS key_group,
        FLOOR(status_code_share_2) AS status_code_share_2_whole,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM scored
    WHERE key IS NOT NULL
),
enriched AS (
    SELECT
        *,
        COUNT(status_code_sum_1h) OVER (PARTITION BY host) AS host_count_status_code_sum_1h,
        AVG(status_code_sum_1h) OVER w_running AS status_code_sum_1h_avg_15m,
        AVG(status_code_share) OVER w_recent AS status_code_share_avg_2h
    FROM filtered
    WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        SUM(status_code_share_2) OVER w_hist AS status_code_share_2_sum_30m,
        MAX(status_code_sum_1h) OVER (PARTITION BY host) AS host_max_status_code_sum_1h
    FROM enriched
    WINDOW w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    SELECT host
    FROM with_windows
    INTERSECT
    SELECT host
    FROM app_logs
    WHERE host = 'h4'
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.host = k.host
),
metrics AS (
    SELECT
        *,
        AVG(status_code_share_avg_2h) OVER (PARTITION BY key) AS key_avg_status_code_share_avg_2h,
        AVG(status_code_share_2_sum_30m) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_share_2_sum_30m_avg_2h,
        DENSE_RANK() OVER w_part AS status_code_share_2_sum_30m_dense_rank,
        DENSE_RANK() OVER w_hist AS status_code_sum_1h_dense_rank
    FROM verified
    WINDOW w_part AS (ORDER BY status_code_share_2_sum_30m), w_hist AS (PARTITION BY key ORDER BY status_code_sum_1h)
)
SELECT
    *,
    MIN(status_code_share_avg_2h) OVER (PARTITION BY host) AS host_min_status_code_share_avg_2h,
    SUM(key_avg_status_code_share_avg_2h) OVER (PARTITION BY service, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS key_avg_status_code_share_avg_2h_sum_15m
FROM metrics;
