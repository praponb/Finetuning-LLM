SELECT launch_date
FROM products
WHERE cost < 380
UNION ALL
SELECT hire_date
FROM employees
ORDER BY launch_date
