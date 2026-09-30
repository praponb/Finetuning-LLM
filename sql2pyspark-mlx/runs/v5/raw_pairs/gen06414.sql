SELECT store_id, AVG(units) AS avg_units
FROM store_sales
WHERE product_id IN (1, 4, 8, 9)
GROUP BY store_id
ORDER BY store_id DESC, avg_units
