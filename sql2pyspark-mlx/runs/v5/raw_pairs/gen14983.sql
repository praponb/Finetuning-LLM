SELECT sale_date
FROM store_sales
WHERE store_id IN (1, 2)
INTERSECT
SELECT hire_date
FROM employees
ORDER BY sale_date
