WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS elem
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share,
        DENSE_RANK() OVER (ORDER BY price DESC) AS price_dense_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY price ASC NULLS LAST) AS price_cume_dist
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, price * price_dense_rank AS price_times_price_dense_rank
    FROM scored
    WHERE promo IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN qty >= 4 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    CASE WHEN price_dense_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END AS price_dense_rank_tier,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    SUM(qty_share) FILTER (WHERE promo = 'WELCOME5') AS welcome5_total_qty_share,
    COUNT(DISTINCT promo) AS distinct_promos
FROM filtered
GROUP BY qty_tier, price_dense_rank_tier
ORDER BY welcome5_total_qty_share DESC NULLS LAST;
