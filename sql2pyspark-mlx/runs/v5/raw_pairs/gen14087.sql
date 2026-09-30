SELECT id, order_id, product_id, quantity, COALESCE(quantity, 0) AS quantity_filled FROM order_items;
