SELECT launch_date
FROM products
UNION
SELECT hire_date AS launch_date
FROM employees
ORDER BY launch_date
