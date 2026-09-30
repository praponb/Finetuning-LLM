WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, INLINE(items)
    FROM carts
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        qty,
        price,
        promo,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY qty DESC) qty_percent_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS LAST) qty_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS LAST) price_percent_rank
    FROM flattened
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(promo) promo_length
    FROM metrics
    WHERE price_percent_rank <= 0.49
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        product_id,
        CASE WHEN price_percent_rank >= 0.82 THEN 'LARGE' ELSE 'SMALL' END AS price_percent_rank_tier,
        AVG(price_percent_rank) FILTER (WHERE promo = 'SPRING10') AS spring10_avg_price_percent_rank,
        PERCENTILE_APPROX(price_percent_rank, 0.5) AS price_percent_rank_p50,
        MIN(qty_dense_rank) AS min_qty_dense_rank,
        COUNT(*) FILTER (WHERE promo = 'WELCOME5') AS welcome5_rows,
        SUM(CASE WHEN promo = 'VIP' THEN qty_percent_rank ELSE 0 END) AS vip_qty_percent_rank,
        SUM(CASE WHEN promo = 'WELCOME5' THEN price_percent_rank ELSE 0 END) AS welcome5_price_percent_rank
    FROM valid
    GROUP BY ROLLUP (product_id, CASE WHEN price_percent_rank >= 0.82 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    product_id,
    price_percent_rank_tier,
    spring10_avg_price_percent_rank,
    price_percent_rank_p50,
    welcome5_rows,
    vip_qty_percent_rank
FROM summary
ORDER BY welcome5_rows DESC NULLS LAST, price_percent_rank_p50;
