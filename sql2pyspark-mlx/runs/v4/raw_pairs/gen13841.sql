SELECT * FROM (SELECT manager_id, dept_id, level, salary FROM employees) src PIVOT (SUM(salary) AS value_sum FOR level IN ('L4', 'L1', 'L5'))
