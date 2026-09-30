SELECT product_id, COUNT(DISTINCT units) AS unique_stores
FROM store_sales
WHERE store_id IN (1, 4, 5) AND units BETWEEN 21 AND 26
GROUP BY 1
ORDER BY product_id;
