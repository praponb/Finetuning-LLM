SELECT id, order_id, quantity FROM order_items QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) = 1;
