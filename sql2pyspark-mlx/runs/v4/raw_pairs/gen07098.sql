SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    ship_to,
    attributes,
    SIZE(items) AS num_items,
    FORALL(items, i -> i.qty <= 5) AS all_small_qty,
    ship_to.country AS ship_country
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'VIP');
