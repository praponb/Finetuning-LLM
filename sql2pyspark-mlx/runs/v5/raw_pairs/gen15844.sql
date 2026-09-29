SELECT *, TRIM(email) AS clean_email FROM customers WHERE signup_date > '2025-08-01' ORDER BY clean_email ASC, id ASC LIMIT 10;
