SELECT opened_date, 'account' AS source FROM accounts WHERE customer_id = 12 UNION SELECT signup_date, 'customer' AS source FROM customers;
