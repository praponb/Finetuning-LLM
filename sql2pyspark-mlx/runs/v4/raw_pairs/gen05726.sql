SELECT store_id, COUNT(*) AS total_count, MIN(units) AS min_units FROM store_sales GROUP BY store_id HAVING total_count >= 3;
