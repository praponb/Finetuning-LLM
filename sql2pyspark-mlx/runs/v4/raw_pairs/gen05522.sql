SELECT store_id FROM store_sales WHERE revenue >= 4000 UNION SELECT store_id FROM store_sales WHERE store_id = 3 ORDER BY store_id
