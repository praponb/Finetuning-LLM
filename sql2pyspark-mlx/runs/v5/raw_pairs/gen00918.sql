SELECT unit_price, product_id, quantity, COALESCE(unit_price, 0) unit_price_filled FROM order_items WHERE NOT (unit_price <= 240 AND order_id IN (4, 18, 39));
