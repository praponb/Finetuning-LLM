WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.created_at,
        elem.price,
        elem.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) e AS elem
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at_2
    FROM exploded
    WHERE promo IS DISTINCT FROM 'SPRING10'
      AND price BETWEEN 160 AND 240
),
enriched AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (PARTITION BY promo ORDER BY days_since_created_at NULLS LAST) AS days_since_created_at_dense_rank
    FROM base
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(price) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_max_1h,
        AVG(days_since_created_at_dense_rank) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS days_since_created_at_dense_rank_avg_15m
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN days_since_created_at_2 >= 84 THEN 'LARGE' ELSE 'SMALL' END AS days_since_created_at_2_tier,
    COUNT(*) AS event_count,
    SUM(CASE WHEN promo = 'VIP' THEN days_since_created_at_dense_rank_avg_15m ELSE 0 END) AS vip_days_since_created_at_dense_rank_avg_15m,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY CUBE (DATE_TRUNC('DAY', created_at), CASE WHEN days_since_created_at_2 >= 84 THEN 'LARGE' ELSE 'SMALL' END);
