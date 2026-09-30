SELECT
    YEAR(signup_date) AS signup_year,
    segment,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END) AS uk_count,
    MIN(age) AS min_age
FROM customers
WHERE segment = 'SMB' AND LOWER(name) LIKE '%vy %'
GROUP BY YEAR(signup_date), segment;
