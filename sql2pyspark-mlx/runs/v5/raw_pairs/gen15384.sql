SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    ship_to,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    attributes['gift'] AS gift,
    SIZE(FILTER(items, x -> x.qty >= 3)) AS bulk_lines
FROM carts;
