SELECT store_id, COUNT(DISTINCT revenue) AS unique_products, MIN(units) AS min_units
FROM store_sales
GROUP BY store_id
ORDER BY unique_products, store_id DESC
LIMIT 1
