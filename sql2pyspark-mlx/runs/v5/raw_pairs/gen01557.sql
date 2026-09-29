SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, SUM(units) AS total_units, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE product_id IN (2, 6, 10) AND revenue > 3300
GROUP BY DATE_TRUNC('MONTH', sale_date)
HAVING ROUND(STDDEV(revenue), 2) < 54
ORDER BY sale_month
