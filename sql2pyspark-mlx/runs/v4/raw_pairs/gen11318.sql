SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, SUM(units) AS total_units
FROM store_sales
WHERE product_id = 4 OR units >= 37
GROUP BY sale_month
HAVING COUNT(*) > 2
ORDER BY total_units DESC;
