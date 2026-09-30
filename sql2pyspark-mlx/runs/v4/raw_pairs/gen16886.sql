WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS status_code_cume_dist,
        AVG(status_code) OVER w_ordered AS status_code_avg_15m,
        MAX(status_code) OVER w_ordered AS status_code_max_15m,
        DENSE_RANK() OVER (ORDER BY status_code) AS status_code_dense_rank
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        value,
        status_code_cume_dist,
        status_code_avg_15m,
        status_code_max_15m,
        status_code_dense_rank,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code_dense_rank DESC NULLS FIRST) AS status_code_dense_rank_dense_rank,
        ROUND(status_code_avg_15m / SUM(status_code_avg_15m) OVER (PARTITION BY attr_key), 4) AS status_code_avg_15m_share
    FROM scored
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(status_code_avg_15m_share) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m_share_max_15m,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code_dense_rank DESC NULLS LAST) AS status_code_dense_rank_dense_rank_2
    FROM with_windows
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        ROUND(AVG(status_code_cume_dist), 2) AS avg_status_code_cume_dist,
        COUNT(*) AS row_count,
        MIN(log_ts) AS first_log_ts,
        ROUND(AVG(status_code_dense_rank_dense_rank), 2) AS avg_status_code_dense_rank_dense_rank
    FROM enriched
    GROUP BY DATE_TRUNC('DAY', log_ts)
)
SELECT
    activity_day,
    avg_status_code_cume_dist,
    first_log_ts
FROM grouped;
