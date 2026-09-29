SELECT CASE WHEN cost >= 780 THEN 'high' WHEN cost >= 150 THEN 'mid' ELSE 'low' END AS cost_bucket, MIN(launch_date) AS min_launch_date, ROUND(AVG(cost), 2) AS avg_cost
FROM products
WHERE category = 'Fashion'
GROUP BY 1;
