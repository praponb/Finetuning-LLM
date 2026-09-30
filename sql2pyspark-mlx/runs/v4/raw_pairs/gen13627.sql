SELECT opened_date, 'account' AS source FROM accounts UNION ALL SELECT signup_date, 'customer' AS source FROM customers WHERE is_active;
