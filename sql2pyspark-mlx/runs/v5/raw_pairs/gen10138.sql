WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS line
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(price * 0.9, 2) AS price_scaled,
        COALESCE(promo, 'NONE') AS promo_clean
    FROM exploded
    WHERE price <= 80
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY price_scaled NULLS FIRST) AS price_scaled_dense_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY price_scaled NULLS FIRST) AS price_scaled_dense_rank_2,
        MAX(price_scaled) OVER (PARTITION BY promo) AS promo_max_price_scaled,
        DENSE_RANK() OVER (ORDER BY price_scaled DESC NULLS FIRST) AS price_scaled_dense_rank_3,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY price_scaled NULLS LAST) AS price_scaled_cume_dist
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        price_scaled_dense_rank_2,
        promo_max_price_scaled,
        price_scaled_cume_dist,
        MAX(price_scaled_dense_rank_3) OVER w_recent AS promo_max_price_scaled_dense_rank_3,
        SUM(promo_max_price_scaled) OVER w_recent AS promo_sum_promo_max_price_scaled,
        RANK() OVER (PARTITION BY promo ORDER BY price_scaled_dense_rank_3) AS price_scaled_dense_rank_3_rank,
        ROUND(price_scaled_dense_rank_3 / SUM(price_scaled_dense_rank_3) OVER (PARTITION BY promo), 4) AS price_scaled_dense_rank_3_share,
        MAX(price_scaled_dense_rank_3) OVER w_recent AS promo_max_price_scaled_dense_rank_3_2
    FROM scored
    WINDOW w_recent AS (PARTITION BY promo)
)
SELECT
    id,
    price_scaled_dense_rank_2,
    promo_max_price_scaled,
    price_scaled_cume_dist,
    price_scaled_dense_rank_3_rank,
    price_scaled_dense_rank_3_share,
    promo_max_price_scaled_dense_rank_3_2
FROM enriched
ORDER BY price_scaled_cume_dist DESC NULLS FIRST;
