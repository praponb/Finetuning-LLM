SELECT start_date FROM subscriptions WHERE customer_id IS NULL EXCEPT SELECT hire_date FROM employees
