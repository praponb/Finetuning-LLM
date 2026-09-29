WITH exploded AS (
    SELECT
        id,
        created_at,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, v
),
cleaned AS (
    SELECT
        *,
        HOUR(created_at) AS created_at_hour,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM exploded
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 02:00:00')
      AND k IS DISTINCT FROM 'device'
),
scored AS (
    SELECT
        id,
        k,
        created_at_hour,
        SUM(created_at_hour) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_hour_sum_15m,
        AVG(created_at_hour) OVER (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_hour_avg_1h,
        MAX(created_at_hour) OVER (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_hour_max_30m
    FROM cleaned
),
valid AS (
    SELECT *, created_at_hour_avg_1h - created_at_hour_max_30m AS created_at_hour_avg_1h_minus_created_at_hour_max_30m
    FROM scored
    WHERE k IS NOT NULL
),
filtered AS (
    SELECT
        k,
        created_at_hour_sum_15m,
        created_at_hour_avg_1h,
        created_at_hour_max_30m,
        ROUND(created_at_hour_max_30m, 0) AS created_at_hour_max_30m_rounded,
        ROUND(created_at_hour_avg_1h, 1) AS created_at_hour_avg_1h_rounded,
        COALESCE(k, 'OTHER') AS k_filled
    FROM valid
    WHERE k IS NOT NULL
),
scored_carts AS (
    SELECT
        created_at_hour_avg_1h,
        created_at_hour_max_30m,
        created_at_hour_max_30m_rounded,
        created_at_hour_avg_1h_rounded,
        SUM(created_at_hour_sum_15m) OVER (PARTITION BY k) AS k_sum_created_at_hour_sum_15m,
        RANK() OVER (ORDER BY created_at_hour_avg_1h) AS created_at_hour_avg_1h_rank,
        CUME_DIST() OVER (PARTITION BY k ORDER BY created_at_hour_max_30m_rounded DESC NULLS FIRST) AS created_at_hour_max_30m_rounded_cume_dist
    FROM filtered
),
filtered_carts AS (
    SELECT
        created_at_hour_avg_1h,
        created_at_hour_max_30m_rounded,
        created_at_hour_avg_1h_rounded,
        k_sum_created_at_hour_sum_15m,
        created_at_hour_avg_1h_rank,
        created_at_hour_max_30m_rounded_cume_dist,
        created_at_hour_avg_1h_rank - created_at_hour_max_30m_rounded AS created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded
    FROM scored_carts
    WHERE created_at_hour_avg_1h_rounded <= 54
)
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded DESC NULLS LAST) AS created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded_dense_rank,
    PERCENT_RANK() OVER (ORDER BY created_at_hour_avg_1h_rank DESC NULLS FIRST) AS created_at_hour_avg_1h_rank_percent_rank,
    PERCENT_RANK() OVER (ORDER BY created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded NULLS FIRST) AS created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded_percent_rank
FROM filtered_carts
