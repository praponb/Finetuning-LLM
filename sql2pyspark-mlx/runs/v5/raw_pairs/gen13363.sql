SELECT
    id,
    customer_id,
    created_at,
    ship_to,
    ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
    ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown
FROM carts
WHERE HOUR(created_at) BETWEEN 12 AND 15;
