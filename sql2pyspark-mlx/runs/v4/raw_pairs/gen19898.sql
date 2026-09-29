SELECT
    id,
    customer_id,
    created_at,
    items,
    ship_to,
    attributes,
    attributes['utm_source'] AS utm_source,
    SIZE(items) AS num_items,
    ship_to.zip AS ship_zip
FROM carts
