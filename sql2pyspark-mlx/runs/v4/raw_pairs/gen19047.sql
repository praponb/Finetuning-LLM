SELECT store_id, AVG(units) AS avg_units
FROM store_sales
GROUP BY store_id
ORDER BY store_id DESC
LIMIT 1
