SELECT id, order_id, unit_price, product_id, FIRST_VALUE(product_id) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS first_product_id FROM order_items ORDER BY unit_price
