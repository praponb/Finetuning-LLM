SELECT
    segment,
    MONTH(signup_date) AS signup_month,
    MIN(age) AS min_age,
    ROUND((100 * SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_uk
FROM customers
GROUP BY segment, MONTH(signup_date)
ORDER BY pct_uk DESC, segment, signup_month
LIMIT 1
