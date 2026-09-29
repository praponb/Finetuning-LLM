SELECT id, opened_date, store_name FROM stores WHERE opened_date = (SELECT MAX(opened_date) FROM stores WHERE store_name IS NOT NULL) ORDER BY 3 DESC, id LIMIT 5
