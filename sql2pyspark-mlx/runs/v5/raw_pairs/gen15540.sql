SELECT * FROM (SELECT store_id, COUNT(*) AS total_count, MIN(quantity) AS min_quantity FROM inventory GROUP BY store_id) AS agg WHERE total_count < 5 ORDER BY 2 DESC
