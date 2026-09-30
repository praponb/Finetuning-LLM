WITH valid_customers AS (
    SELECT *, age + credit_limit AS age_plus_credit_limit
    FROM customers
    WHERE age = 40 AND name LIKE 'Ka%' AND UPPER(TRIM(email)) = 'IVY@GMAIL.COM'
),
Summary AS (
    SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, COUNT(credit_limit) AS credit_limit_count, SUM(age_plus_credit_limit) AS total_age_plus_credit_limit
    FROM valid_customers
    GROUP BY DATE_TRUNC('MONTH', signup_date)
)
SELECT signup_month, credit_limit_count, total_age_plus_credit_limit
FROM Summary
WHERE credit_limit_count < 6
ORDER BY signup_month
LIMIT 20;
