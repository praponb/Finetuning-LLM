SELECT
    country,
    MAX(age) AS max_age,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
FROM customers
WHERE credit_limit IS NOT NULL
GROUP BY country
ORDER BY row_count DESC;
