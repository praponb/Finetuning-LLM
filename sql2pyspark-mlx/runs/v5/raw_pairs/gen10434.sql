WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t0.id,
        t0.created_at,
        line.product_id,
        line.price,
        line.promo
    FROM carts t0
    LATERAL VIEW EXPLODE(t0.items) t AS line
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        product_id,
        MAX(price) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS price_max_15m,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share
    FROM exploded
    WHERE promo = 'WELCOME5'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        created_at,
        product_id,
        price_max_15m,
        product_id_share,
        DENSE_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank,
        AVG(price_max_15m) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_max_15m_avg_30m
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(product_id_share) AS product_id_share_whole,
        product_id * price_max_15m AS product_id_times_price_max_15m
    FROM metrics
    WHERE created_at >= DATE '2025-03-01'
      AND product_id < 8
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        created_at,
        product_id,
        price_max_15m,
        price_max_15m_avg_30m,
        product_id_times_price_max_15m,
        PERCENT_RANK() OVER (ORDER BY price_max_15m DESC) AS price_max_15m_percent_rank,
        SUM(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS product_id_sum_2h
    FROM valid
)
-- Grouped metrics
SELECT
    product_id,
    CASE WHEN price_max_15m_percent_rank >= 0.71 THEN 'HIGH' ELSE 'LOW' END AS price_max_15m_percent_rank_tier,
    CASE WHEN price_max_15m >= 187 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_max_15m_tier,
    ROUND(AVG(product_id), 2) AS avg_product_id,
    COUNT(*) AS row_count,
    RANK() OVER (PARTITION BY product_id ORDER BY ROUND(AVG(product_id), 2) DESC) AS avg_product_id_rank
FROM enriched_carts
GROUP BY product_id, CASE WHEN price_max_15m_percent_rank >= 0.71 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN price_max_15m >= 187 THEN 'PREMIUM' ELSE 'STANDARD' END;
