SELECT signup_date, credit_limit
FROM customers
WHERE UPPER(TRIM(name)) = 'DAVID LEE'
ORDER BY credit_limit DESC
