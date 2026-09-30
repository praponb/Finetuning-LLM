SELECT id, level, hire_date, salary, SUM(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary FROM employees;
