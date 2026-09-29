SELECT id, level, dept_id, hire_date, salary, SUM(salary) OVER (PARTITION BY level, dept_id ORDER BY hire_date, id) AS running_salary FROM employees;
