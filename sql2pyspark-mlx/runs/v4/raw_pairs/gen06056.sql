SELECT level, SUM(salary) AS total_salary FROM employees WHERE dept_id IN (29, 32, 35, 36) AND is_remote GROUP BY level HAVING total_salary >= 820000 ORDER BY level
