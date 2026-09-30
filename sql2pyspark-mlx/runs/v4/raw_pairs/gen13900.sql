SELECT name, email, age, DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date FROM customers WHERE credit_limit BETWEEN 4500 AND 9100 AND signup_date <= '2024-07-24';
