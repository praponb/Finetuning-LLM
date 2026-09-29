SELECT id, signup_date, DENSE_RANK() OVER (ORDER BY signup_date) signup_date_dense_rank
FROM customers
