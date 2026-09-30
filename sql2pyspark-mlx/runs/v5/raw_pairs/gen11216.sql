SELECT signup_date, age FROM customers WHERE country NOT IN ('DE', 'TH', 'UK') UNION SELECT signup_date, age FROM customers WHERE email IS NOT NULL
