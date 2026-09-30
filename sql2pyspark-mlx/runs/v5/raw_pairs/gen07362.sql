SELECT * FROM (SELECT job_title, level, salary FROM employees) PIVOT (SUM(salary) FOR level IN ('L5', 'L2'));
