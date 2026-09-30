SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
    ship_to.city AS ship_city,
    attributes['utm_source'] AS utm_source,
    ship_to.country AS ship_country
FROM carts
