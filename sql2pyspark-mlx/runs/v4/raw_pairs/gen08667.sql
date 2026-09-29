SELECT id, product_id, quantity, order_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num FROM order_items) AS ranked WHERE row_num <= 2
