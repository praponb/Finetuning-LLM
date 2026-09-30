SELECT id, signup_date, credit_limit, LAG(credit_limit, 1) OVER (ORDER BY signup_date, id) AS prev_credit_limit FROM customers
