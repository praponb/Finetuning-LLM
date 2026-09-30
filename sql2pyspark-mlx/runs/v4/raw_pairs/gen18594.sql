SELECT MONTH(signup_date) AS signup_month, MIN(signup_date) AS first_signup_date
FROM customers
WHERE city <> 'Tokyo'
GROUP BY MONTH(signup_date)
ORDER BY first_signup_date, signup_month
LIMIT 10
