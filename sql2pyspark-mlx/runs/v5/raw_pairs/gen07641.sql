SELECT
    id,
    customer_id,
    items,
    attributes,
    ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
    ship_to.zip AS ship_zip,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
    attributes['device'] AS device,
    COALESCE(attributes['device'], 'direct') AS device_or_default
FROM carts
WHERE customer_id IS NULL
