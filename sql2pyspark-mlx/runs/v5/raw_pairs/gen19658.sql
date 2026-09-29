SELECT country, DATE_TRUNC('MONTH', signup_date) AS signup_month, COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
FROM customers
WHERE age = 42
GROUP BY 1, 2
ORDER BY country
