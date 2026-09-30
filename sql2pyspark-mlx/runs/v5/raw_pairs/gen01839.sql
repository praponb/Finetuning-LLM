SELECT signup_date FROM customers WHERE email LIKE '%@outlook.com' EXCEPT SELECT signup_date FROM customers WHERE MONTH(signup_date) IN (1, 4, 10);
