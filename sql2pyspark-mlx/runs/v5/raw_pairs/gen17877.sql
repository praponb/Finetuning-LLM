SELECT country, CASE WHEN age >= 61 THEN 'high' WHEN age >= 37 THEN 'mid' ELSE 'low' END AS age_bucket, COUNT(age) AS age_count FROM customers GROUP BY 1, 2
