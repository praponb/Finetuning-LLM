SELECT product_id FROM store_sales EXCEPT ALL SELECT product_id FROM store_sales WHERE store_id IN (1, 3, 5)
