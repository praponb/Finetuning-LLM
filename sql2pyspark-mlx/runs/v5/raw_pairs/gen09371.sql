SELECT 'customer' source, signup_date
FROM customers
WHERE age BETWEEN 31 AND 59
UNION
SELECT 'employee' source, hire_date
FROM employees
ORDER BY signup_date;
