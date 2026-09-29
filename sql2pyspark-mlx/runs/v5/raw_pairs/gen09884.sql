SELECT YEAR(opened_date) AS opened_year, SUM(sqft) AS total_sqft, SUM(SUM(sqft)) OVER (ORDER BY YEAR(opened_date)) AS cumulative_sqft
FROM stores
GROUP BY YEAR(opened_date)
ORDER BY cumulative_sqft DESC;
