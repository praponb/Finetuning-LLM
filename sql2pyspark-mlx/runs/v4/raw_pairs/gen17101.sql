WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        INLINE(items)
    FROM carts
)
-- Ranking and gap features
SELECT
    id,
    coupon_codes,
    product_id,
    qty,
    price,
    promo,
    ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) price_share,
    ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) price_share_2,
    AVG(product_id) OVER w_ordered promo_avg_product_id,
    ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) product_id_share,
    ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) product_id_share_2
FROM flattened
WINDOW w_ordered AS (PARTITION BY promo);
