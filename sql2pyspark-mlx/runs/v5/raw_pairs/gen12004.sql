SELECT MAX(signup_date) AS last_signup_date
FROM customers
WHERE credit_limit IS NOT NULL
ORDER BY 1 DESC;
