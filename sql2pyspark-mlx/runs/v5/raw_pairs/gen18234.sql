SELECT id, product_id, store_id FROM store_sales WHERE (revenue >= 1500 AND MONTH(sale_date) IN (1, 6, 10)) OR revenue >= 3400 ORDER BY store_id DESC, id ASC LIMIT 5;
