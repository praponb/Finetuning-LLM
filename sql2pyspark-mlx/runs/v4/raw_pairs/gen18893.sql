SELECT CASE WHEN sqft >= 14086 THEN 'high' WHEN sqft >= 3916 THEN 'mid' ELSE 'low' END AS sqft_bucket, AVG(sqft) AS avg_sqft
FROM stores
GROUP BY sqft_bucket
HAVING AVG(sqft) > 5800
ORDER BY sqft_bucket DESC
LIMIT 1
