SELECT id, customer_id, created_at, items, ship_to, ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device, attributes['gift'] AS gift FROM carts WHERE customer_id IN (17, 19, 25)
