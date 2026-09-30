SELECT product_id, AVG(units) AS avg_units
FROM store_sales
WHERE sale_date BETWEEN '2023-09-13' AND '2024-10-16'
GROUP BY product_id
ORDER BY product_id DESC, avg_units;
