SELECT id, signup_date, age, LAG(age, 1) OVER (ORDER BY signup_date, id) AS prev_age FROM customers WHERE credit_limit >= 6400;
