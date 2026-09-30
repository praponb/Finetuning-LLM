SELECT id, level, salary, MAX(salary) OVER (PARTITION BY level) AS level_max_salary FROM employees;
