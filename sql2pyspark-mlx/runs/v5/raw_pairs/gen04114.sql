SELECT id, country, signup_date, credit_limit, LAG(credit_limit, 1) OVER (PARTITION BY country ORDER BY signup_date, id) AS prev_credit_limit FROM customers;
