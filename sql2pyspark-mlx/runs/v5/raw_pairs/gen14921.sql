WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        qty,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(price) OVER (PARTITION BY value) AS value_min_price,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share,
        ROUND(price / SUM(price) OVER (PARTITION BY value, promo), 4) AS price_share_2,
        PERCENT_RANK() OVER (PARTITION BY value ORDER BY price DESC) AS price_percent_rank,
        RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS FIRST) AS qty_rank
    FROM exploded_carts
),
metrics_carts AS (
    -- Ranking and gap features
    SELECT
        id,
        price,
        promo,
        value,
        price_share_2,
        price_percent_rank,
        qty_rank,
        COUNT(price_percent_rank) OVER (PARTITION BY promo) AS promo_count_price_percent_rank,
        RANK() OVER (PARTITION BY promo ORDER BY qty_rank NULLS LAST) AS qty_rank_rank
    FROM metrics
    WHERE qty <= 5
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    value,
    metric,
    reading
FROM (
    SELECT
        value,
        price,
        price_share_2,
        price_percent_rank
    FROM metrics_carts
) src
UNPIVOT (
    reading FOR metric IN (price AS PRICE_VAL, price_share_2, price_percent_rank AS PRICE_PERCENT_RANK_VAL)
);
