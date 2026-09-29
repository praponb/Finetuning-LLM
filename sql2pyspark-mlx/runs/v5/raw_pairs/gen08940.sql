SELECT CASE WHEN cost >= 590 THEN 'high' WHEN cost >= 350 THEN 'mid' ELSE 'low' END AS cost_bucket, MIN(launch_date) AS first_launch_date, COUNT(*) AS row_count
FROM products
WHERE MONTH(launch_date) IN (3, 10, 11) AND price BETWEEN 320 AND 990
GROUP BY cost_bucket;
