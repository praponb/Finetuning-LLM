SELECT product_id FROM store_sales WHERE YEAR(sale_date) >= 2024 UNION SELECT product_id FROM store_sales WHERE store_id = 5
