SELECT quantity FROM order_items WHERE product_id IS NULL UNION ALL SELECT quantity FROM order_items WHERE quantity IS NULL;
