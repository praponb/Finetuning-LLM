SELECT signup_date
FROM customers
WHERE age <> 38 AND UPPER(TRIM(email)) = 'EMMA@OUTLOOK.COM'
ORDER BY signup_date DESC
