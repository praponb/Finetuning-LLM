SELECT id, dept_id, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS max_salary FROM employees;
