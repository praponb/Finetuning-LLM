SELECT MIN(sale_date) AS first_sale_date, ROUND(AVG(units), 2) AS avg_units, MAX(units) AS max_units
FROM store_sales
WHERE units BETWEEN 24 AND 34
ORDER BY first_sale_date;
