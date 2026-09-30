SELECT product_id, store_id FROM store_sales EXCEPT ALL SELECT product_id, store_id FROM store_sales WHERE product_id IN (1, 3, 10)
