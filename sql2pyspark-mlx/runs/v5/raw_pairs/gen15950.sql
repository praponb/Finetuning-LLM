SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, AVG(units) AS avg_units FROM store_sales GROUP BY 1 HAVING avg_units >= 22 ORDER BY sale_month
