SELECT
    id,
    customer_id,
    coupon_codes,
    attributes,
    EXISTS(items, li -> li.price > 100) AS has_premium_item,
    SIZE(items) AS num_items
FROM carts;
