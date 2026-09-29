SELECT hire_date
FROM employees
WHERE YEAR(hire_date) = 2025
UNION
SELECT delivered_date
FROM shipments
WHERE shipped_date BETWEEN '2023-08-01' AND '2024-02-01'
ORDER BY hire_date DESC
