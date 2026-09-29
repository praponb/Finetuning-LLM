SELECT * FROM (SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, level, salary FROM employees WHERE manager_id IS NULL) PIVOT (MAX(salary) FOR level IN ('L1', 'L2', 'L3', 'L5'))
