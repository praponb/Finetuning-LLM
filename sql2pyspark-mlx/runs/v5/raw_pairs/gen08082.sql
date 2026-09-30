SELECT
    CASE WHEN age >= 72 THEN 'high' WHEN age >= 28 THEN 'mid' ELSE 'low' END AS age_bucket,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    SUM(SUM(age)) OVER (ORDER BY CASE WHEN age >= 72 THEN 'high' WHEN age >= 28 THEN 'mid' ELSE 'low' END) AS cumulative_age
FROM customers
GROUP BY CASE WHEN age >= 72 THEN 'high' WHEN age >= 28 THEN 'mid' ELSE 'low' END;
