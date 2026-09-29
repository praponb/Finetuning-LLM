SELECT MONTH(sale_date) AS sale_month, SUM(units) AS total_units, ROUND(SUM(units) * 100.0 / SUM(SUM(units)) OVER (), 2) AS pct_of_total
FROM store_sales
GROUP BY MONTH(sale_date)
ORDER BY sale_month;
