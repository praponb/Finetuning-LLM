SELECT id, product_id, quantity, unit_price, LEAD(unit_price, 2) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_unit_price FROM order_items ORDER BY unit_price;
