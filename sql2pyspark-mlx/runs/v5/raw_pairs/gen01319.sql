SELECT id, is_active, DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date FROM customers;
