SELECT store_id, COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE quantity BETWEEN 72 AND 74
GROUP BY store_id
HAVING COUNT(DISTINCT quantity) > 4
ORDER BY unique_products DESC;
