SELECT id, sale_date FROM store_sales WHERE store_id IN (2, 5, 6) INTERSECT SELECT id, sale_date FROM store_sales WHERE units <= 27 ORDER BY sale_date
