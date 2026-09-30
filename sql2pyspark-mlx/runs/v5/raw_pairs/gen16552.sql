SELECT
    id,
    coupon_codes,
    ship_to,
    AGGREGATE(items, 0D, (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS items_total,
    COALESCE(attributes['device'], 'direct') AS device_or_default,
    TRANSFORM(
        FILTER(items, i -> i.promo IS NOT NULL),
        i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'gift') AS has_gift
FROM carts;
