SELECT
    id,
    customer_id,
    ship_to,
    COALESCE(attributes['device'], 'direct') AS device_or_default,
    SIZE(attributes) AS num_attributes
FROM carts
