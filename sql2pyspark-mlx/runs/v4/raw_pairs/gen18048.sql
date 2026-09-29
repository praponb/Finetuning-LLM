SELECT level, max_salary FROM (SELECT level, MAX(salary) AS max_salary FROM employees WHERE is_remote GROUP BY level) AS t WHERE max_salary > 107000 ORDER BY level;
