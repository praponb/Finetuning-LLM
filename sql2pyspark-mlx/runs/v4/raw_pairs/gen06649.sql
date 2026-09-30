SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    ship_to,
    attributes['gift'] AS gift,
    FORALL(items, li -> li.qty <= 5) AS all_small_qty,
    ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
    COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
    SIZE(items) AS num_items
FROM carts
