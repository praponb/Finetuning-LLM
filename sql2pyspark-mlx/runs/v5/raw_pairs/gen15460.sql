WITH top_employees AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS row_num FROM employees) SELECT * FROM top_employees WHERE row_num <= 3 ORDER BY level DESC;
