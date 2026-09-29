SELECT id, quantity, product_id, LAST_VALUE(product_id) OVER (ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_product_id FROM order_items;
