SELECT product_id, MAX(units) AS max_units, ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
WHERE sale_date >= '2025-04-01'
GROUP BY product_id
ORDER BY stddev_units, product_id
LIMIT 50;
