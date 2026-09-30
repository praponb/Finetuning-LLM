SELECT id, order_id, unit_price, product_id FROM (SELECT id, order_id, unit_price, product_id, RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) AS rn FROM order_items) AS r WHERE rn = 1;
