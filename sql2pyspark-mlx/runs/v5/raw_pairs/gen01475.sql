SELECT
    id,
    customer_id,
    ship_to,
    attributes,
    TRANSFORM(items, item -> item.product_id) AS product_ids,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source
FROM carts
