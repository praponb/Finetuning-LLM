SELECT id, customer_id, items, ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source, FORALL(items, item -> item.qty <= 5) AS all_small_qty FROM carts WHERE customer_id IS NOT NULL;
