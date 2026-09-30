SELECT hire_date
FROM employees
WHERE LENGTH(email) < 13
EXCEPT
SELECT opened_date AS hire_date
FROM accounts
WHERE customer_id = 19;
