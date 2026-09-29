SELECT product_id, COUNT(*) AS store_sale_count, MAX(units) AS max_units
FROM store_sales
WHERE revenue BETWEEN 2500 AND 2700
GROUP BY product_id
ORDER BY product_id ASC
LIMIT 5
