SELECT id, product_id, sale_date FROM store_sales WHERE YEAR(sale_date) = 2025 UNION SELECT id, product_id, sale_date FROM store_sales WHERE product_id IN (8, 9, 10)
