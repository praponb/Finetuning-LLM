WITH latest AS (SELECT * FROM order_items QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST) = 1) SELECT id, product_id, quantity, unit_price FROM latest;
