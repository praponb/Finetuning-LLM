SELECT sale_date, product_id, units FROM store_sales WHERE revenue IS NULL AND (product_id = 2 OR product_id IN (1, 5, 6))
