SELECT store_id, SUM(units) AS total_units
FROM store_sales
GROUP BY store_id
ORDER BY store_id
LIMIT 3
