SELECT id, signup_date
FROM customers
WHERE signup_date = (SELECT MIN(signup_date) FROM customers WHERE email IS NOT NULL)
