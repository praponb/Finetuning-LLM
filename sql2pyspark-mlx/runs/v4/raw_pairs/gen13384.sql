SELECT id, order_id, quantity, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) row_num FROM order_items
