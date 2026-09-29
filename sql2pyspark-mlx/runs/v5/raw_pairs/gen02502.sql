SELECT *, CAST(salary AS INT) AS salary_int, CAST(salary AS INT) AS salary_int_2 FROM employees WHERE hire_date >= '2025-02-01' ORDER BY email DESC, level DESC, id LIMIT 10;
