SELECT store_id, COUNT(DISTINCT units) AS unique_stores FROM store_sales WHERE units BETWEEN 30 AND 39 AND product_id IN (3, 7, 8, 10) GROUP BY 1 ORDER BY unique_stores DESC, store_id LIMIT 1;
