SELECT id, product_id, order_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC, id) AS row_num FROM order_items ORDER BY row_num, product_id DESC
