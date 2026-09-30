SELECT DATE_FORMAT(sale_date, 'yyyy-MM') sale_month, COUNT(*) num_store_sales, ROUND(AVG(units), 2) avg_units
FROM store_sales
WHERE NOT (units BETWEEN 13 AND 23 OR revenue BETWEEN 1100 AND 2700)
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY num_store_sales;
