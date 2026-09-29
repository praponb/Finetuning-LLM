WITH flattened AS (
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
valid AS (
    SELECT *, SUBSTRING(code, 1, 2) AS code_prefix
    FROM flattened
    WHERE created_at >= DATE '2025-03-01'
),
cleaned AS (
    SELECT
        *,
        HOUR(created_at) AS created_at_hour,
        UPPER(code) AS code_upper
    FROM valid
    WHERE created_at > TIMESTAMP '2025-03-02 00:00:00'
      AND code IS NOT NULL
),
matched_keys AS (
    SELECT id
    FROM cleaned
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE customer_id IN (22, 26, 30)
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id
),
metrics AS (
    SELECT
        created_at,
        code_prefix,
        created_at_hour,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY created_at_hour DESC NULLS LAST) AS created_at_hour_dense_rank
    FROM verified
),
enriched AS (
    SELECT
        *,
        MAX(created_at_hour) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_hour_max_2h,
        AVG(created_at_hour) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_hour_avg_30m,
        COUNT(created_at_hour_dense_rank) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_hour_dense_rank_count_15m
    FROM metrics
    WHERE created_at_hour_dense_rank > 4
),
filtered AS (
    SELECT
        created_at,
        code_prefix,
        created_at_hour,
        created_at_hour_avg_30m,
        created_at_hour_dense_rank_count_15m,
        LEAST(created_at_hour_dense_rank_count_15m, created_at_hour_max_2h) AS least_created_at_hour_dense_rank_count_15m_created_at_hour_max_2h
    FROM enriched
    WHERE created_at_hour_max_2h < 47
)
SELECT
    CASE WHEN created_at_hour >= 80 THEN 'LARGE' ELSE 'SMALL' END AS created_at_hour_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    PERCENTILE_APPROX(least_created_at_hour_dense_rank_count_15m_created_at_hour_max_2h, ARRAY(0.25, 0.5, 0.75)) AS least_created_at_hour_dense_rank_count_15m_created_at_hour_max_2h_quartiles,
    MAX(created_at) AS ended_at,
    ROUND(AVG(created_at_hour_avg_30m), 2) AS avg_created_at_hour_avg_30m,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(created_at_hour, ARRAY(0.25, 0.5, 0.75)) AS created_at_hour_quartiles
FROM filtered
GROUP BY ROLLUP (CASE WHEN created_at_hour >= 80 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('HOUR', created_at))
HAVING COUNT(*) > 1
ORDER BY event_count DESC NULLS FIRST;
