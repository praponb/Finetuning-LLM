WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
scored AS (
    SELECT
        *,
        RANK() OVER (ORDER BY status_code) AS status_code_rank,
        RANK() OVER (PARTITION BY host ORDER BY status_code DESC) AS status_code_rank_2,
        MAX(status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h
    FROM exploded
),
scored_app_logs AS (
    SELECT
        *,
        AVG(status_code_rank) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_rank_avg_1h,
        SUM(status_code_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_rank_sum_30m,
        RANK() OVER (ORDER BY status_code_rank_2) AS status_code_rank_2_rank
    FROM scored
),
metrics AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        v,
        status_code_rank,
        status_code_rank_2,
        status_code_max_2h,
        status_code_rank_avg_1h,
        status_code_rank_sum_30m,
        status_code_rank_2_rank,
        AVG(status_code_rank_avg_1h) OVER w_grp AS status_code_rank_avg_1h_avg_30m,
        SUM(status_code_rank_sum_30m) OVER w_hist AS status_code_rank_sum_30m_sum_1h,
        ROUND(status_code_rank_avg_1h / SUM(status_code_rank_avg_1h) OVER (PARTITION BY level), 4) AS status_code_rank_avg_1h_share,
        ROUND(status_code_rank_2 / SUM(status_code_rank_2) OVER (PARTITION BY service), 4) AS status_code_rank_2_share
    FROM scored_app_logs
    WINDOW w_grp AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        *,
        CEIL(status_code_rank_2_share) AS status_code_rank_2_share_whole,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM metrics
    WHERE k IS NOT NULL
      AND service IN ('checkout', 'search')
)
SELECT
    id,
    host,
    level,
    status_code,
    k,
    v,
    status_code_rank,
    status_code_rank_2,
    status_code_rank_avg_1h,
    CASE WHEN k RLIKE '^(region|version)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS k_zone
FROM cleaned;
