SELECT
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    ROUND((100 * SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_jp,
    SUM(CASE WHEN city = 'New York' THEN credit_limit ELSE 0 END) AS new_york_credit_limit,
    SUM(credit_limit) AS total_credit_limit
FROM customers
GROUP BY DATE_TRUNC('MONTH', signup_date)
HAVING COUNT(age) >= 4;
