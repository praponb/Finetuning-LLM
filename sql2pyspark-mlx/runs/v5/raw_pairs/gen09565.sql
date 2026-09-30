SELECT store_id, product_id FROM store_sales WHERE units IS NOT NULL UNION SELECT store_id, product_id FROM store_sales WHERE MONTH(sale_date) IN (1, 2, 8) ORDER BY store_id
