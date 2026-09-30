SELECT DATE_TRUNC('MONTH', sale_date) sale_month, MIN(revenue) min_revenue, ROUND(STDDEV(units), 2) stddev_units
FROM store_sales
WHERE revenue <= 1000
GROUP BY DATE_TRUNC('MONTH', sale_date)
HAVING MIN(revenue) >= 1200
ORDER BY min_revenue, sale_month
LIMIT 20;
