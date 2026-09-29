SELECT
    id,
    items,
    ship_to,
    attributes['device'] AS device,
    AGGREGATE(items, 0.0D, (acc, x) -> acc + x.qty * x.price, acc -> ROUND(acc, 2)) AS cart_value,
    ship_to.city AS ship_city,
    EXISTS(items, x -> x.price > 100) AS has_premium_item,
    COALESCE(attributes['gift'], 'none') AS gift_or_default
FROM carts;
