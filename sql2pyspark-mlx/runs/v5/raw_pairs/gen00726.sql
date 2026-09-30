WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) x
)
SELECT
    id,
    created_at,
    PERCENT_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank,
    COUNT(product_id) OVER (PARTITION BY promo) AS promo_count_product_id
FROM flattened
