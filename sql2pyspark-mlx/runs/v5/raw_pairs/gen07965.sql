SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS row_num
FROM employees
QUALIFY row_num = 1;
