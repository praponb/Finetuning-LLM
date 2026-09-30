SELECT
    id,
    customer_id,
    ship_to,
    attributes,
    EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
    ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price
FROM carts;
