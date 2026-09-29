SELECT id, product_id, unit_price, quantity, COALESCE(quantity, 0) AS quantity_filled FROM order_items;
