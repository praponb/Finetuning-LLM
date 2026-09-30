SELECT opened_date FROM accounts UNION ALL SELECT signup_date FROM customers WHERE is_active = TRUE;
