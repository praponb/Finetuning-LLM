SELECT CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, SUM(CASE WHEN segment = 'Consumer' THEN age ELSE 0 END) AS consumer_age, MIN(credit_limit) AS min_credit_limit
FROM customers
WHERE country NOT IN ('DE', 'UK')
GROUP BY 1
HAVING COUNT(CASE WHEN country = 'JP' THEN 1 END) > 5
ORDER BY signup_cohort DESC
LIMIT 5
