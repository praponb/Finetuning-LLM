SELECT sale_date, 'store_sale' AS source
FROM store_sales
WHERE units BETWEEN 20 AND 21
UNION
SELECT hire_date, 'employee' AS source
FROM employees
WHERE MONTH(hire_date) IN (2, 5, 8)
ORDER BY sale_date;
