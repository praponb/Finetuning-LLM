SELECT id, store_id FROM store_sales WHERE store_id IN (1, 4, 5, 6) AND (units = 34 OR sale_date BETWEEN '2023-04-19' AND '2025-07-01')
