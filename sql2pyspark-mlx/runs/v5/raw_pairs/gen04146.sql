WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) x AS li_item
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        price,
        promo,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY price DESC) AS price_dense_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share
    FROM flattened
    WHERE promo != 'BUNDLE'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        product_id,
        price,
        promo,
        price_share + price_dense_rank AS price_share_plus_price_dense_rank,
        FLOOR(price_share) AS price_share_whole,
        COALESCE(promo, 'N/A') AS promo_filled
    FROM metrics
    WHERE price_share < 0.58
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(price_share_plus_price_dense_rank) OVER (PARTITION BY promo) AS promo_avg_price_share_plus_price_dense_rank,
        CUME_DIST() OVER w_part AS price_share_whole_cume_dist
    FROM valid
    WINDOW w_part AS (PARTITION BY promo ORDER BY price_share_whole DESC NULLS LAST)
)
SELECT
    product_id,
    price,
    promo,
    price_share_plus_price_dense_rank,
    price_share_whole,
    promo_filled,
    promo_avg_price_share_plus_price_dense_rank,
    price_share_whole_cume_dist,
    CASE WHEN promo RLIKE '^(VIP|WELCOME5)$' THEN 'EXTENDED' WHEN promo RLIKE '^(SPRING10|BUNDLE)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS promo_zone
FROM scored
ORDER BY price_share_whole_cume_dist NULLS LAST;
