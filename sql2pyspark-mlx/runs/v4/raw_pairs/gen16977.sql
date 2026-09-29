SELECT * FROM (SELECT level, job_title, salary FROM employees) src PIVOT (SUM(salary) FOR job_title IN ('Manager' AS manager, 'Designer' AS designer, 'Director' AS director))
