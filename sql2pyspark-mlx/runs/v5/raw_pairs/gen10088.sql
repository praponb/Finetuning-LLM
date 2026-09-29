SELECT id, ship_to, attributes, COALESCE(attributes['gift'], 'unknown') AS gift_or_default, TRANSFORM(items, item -> item.product_id) AS product_ids FROM carts;
