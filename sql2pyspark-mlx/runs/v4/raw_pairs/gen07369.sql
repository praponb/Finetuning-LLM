SELECT MONTH(opened_date) AS opened_month, SUM(sqft) AS total_sqft, SUM(SUM(sqft)) OVER (ORDER BY MONTH(opened_date)) AS cumulative_sqft
FROM stores
GROUP BY MONTH(opened_date)
ORDER BY opened_month
LIMIT 10;
