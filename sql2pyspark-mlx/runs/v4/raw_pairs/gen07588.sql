SELECT
    id,
    customer_id,
    SIZE(items) AS num_items,
    attributes['utm_source'] AS utm_source,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    EXISTS(items, x -> x.price > 100) AS has_premium_item
FROM carts
WHERE customer_id IN (7, 12, 25, 36);
