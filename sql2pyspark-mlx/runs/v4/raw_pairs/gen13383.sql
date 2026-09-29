SELECT opened_date, 'store' AS source
FROM stores
UNION
SELECT hire_date, 'employee' AS source
FROM employees;
