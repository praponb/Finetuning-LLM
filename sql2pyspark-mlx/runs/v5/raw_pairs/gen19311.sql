SELECT ROUND(AVG(units), 2) AS avg_units
FROM store_sales
WHERE revenue < 2800
