SELECT id, level, salary, hire_date, FIRST_VALUE(hire_date) OVER (PARTITION BY level ORDER BY salary DESC, id) first_hire_date FROM employees ORDER BY hire_date
