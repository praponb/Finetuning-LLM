SELECT *
FROM customers
WHERE MONTH(signup_date) IN (4, 7, 8) AND LENGTH(email) < 5
