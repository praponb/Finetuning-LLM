SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS pos FROM employees WHERE YEAR(hire_date) >= 2025) AS ranked WHERE pos <= 3 ORDER BY pos DESC;
