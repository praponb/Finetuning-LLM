SELECT region, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
WHERE region IN ('West', 'East') OR opened_date >= '2024-04-08'
GROUP BY region, DATE_FORMAT(opened_date, 'yyyy-MM')
HAVING MAX(sqft) - MIN(sqft) < 74
ORDER BY sqft_range DESC, region, opened_month
LIMIT 10;
