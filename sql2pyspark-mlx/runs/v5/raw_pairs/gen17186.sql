WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT *, ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share
    FROM exploded
    WHERE key IN ('trace', 'region', 'version')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(status_code_share) OVER (PARTITION BY key) AS key_sum_status_code_share,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY status_code_share DESC) AS status_code_share_dense_rank
    FROM scored
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        key,
        attr_value,
        status_code_share,
        DENSE_RANK() OVER w_recent AS status_code_share_dense_rank_dense_rank,
        MAX(status_code_share_dense_rank) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_share_dense_rank_max_15m
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY key ORDER BY status_code_share_dense_rank DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_share_dense_rank_max_15m >= 7 THEN 'LARGE' ELSE 'SMALL' END AS status_code_share_dense_rank_max_15m_tier,
    CASE WHEN status_code_share >= 0.62 THEN 'LARGE' ELSE 'SMALL' END AS status_code_share_tier,
    AVG(status_code) FILTER (WHERE key = 'trace') AS trace_avg_status_code,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    ROUND(AVG(status_code_share), 2) AS avg_status_code_share
FROM enriched
GROUP BY CASE WHEN status_code_share_dense_rank_max_15m >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_code_share >= 0.62 THEN 'LARGE' ELSE 'SMALL' END
HAVING STDDEV_SAMP(status_code) > 0;
