SELECT
    id,
    coupon_codes,
    attributes,
    TRANSFORM(
        FILTER(items, item -> item.promo IS NOT NULL),
        item -> STRUCT(item.product_id AS pid, ROUND(item.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'gift') AS has_gift
FROM carts
WHERE SIZE(coupon_codes) > 2;
