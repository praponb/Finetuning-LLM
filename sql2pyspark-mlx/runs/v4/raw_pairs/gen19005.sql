SELECT product_id FROM store_sales WHERE units > 37 UNION SELECT product_id FROM store_sales WHERE units < 17 ORDER BY product_id
