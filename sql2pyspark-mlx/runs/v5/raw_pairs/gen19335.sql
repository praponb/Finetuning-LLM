SELECT email, 'customer' AS origin FROM customers WHERE email LIKE '%@GMAIL.COM' UNION ALL SELECT email, 'employee' AS origin FROM employees ORDER BY email
