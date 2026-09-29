SELECT id, city, signup_date, credit_limit, LAG(credit_limit, 1) OVER (PARTITION BY city ORDER BY signup_date, id) AS prev_credit_limit FROM customers ORDER BY prev_credit_limit;
