SELECT id, level, hire_date, salary, LEAD(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS next_salary FROM employees WHERE level IN ('L1', 'L5', 'L3');
