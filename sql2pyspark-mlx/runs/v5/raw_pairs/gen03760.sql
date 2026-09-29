SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, CASE WHEN age >= 57 THEN 'high' WHEN age >= 33 THEN 'mid' ELSE 'low' END AS age_bucket, SUM(age) AS total_age
FROM customers
WHERE MONTH(signup_date) IN (1, 2, 3) AND age BETWEEN 46 AND 60 AND age BETWEEN 40 AND 51
GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM'), CASE WHEN age >= 57 THEN 'high' WHEN age >= 33 THEN 'mid' ELSE 'low' END
HAVING SUM(age) < 180
ORDER BY age_bucket, signup_month
LIMIT 10;
