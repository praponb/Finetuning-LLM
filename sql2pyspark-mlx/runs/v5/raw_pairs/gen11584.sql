SELECT id, sale_date FROM store_sales WHERE revenue <= 3600 UNION SELECT id, sale_date FROM store_sales WHERE store_id IN (3, 5)
