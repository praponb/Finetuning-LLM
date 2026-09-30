SELECT opened_date
FROM stores
UNION
SELECT hire_date AS opened_date
FROM employees;
