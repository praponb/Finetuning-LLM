SELECT * FROM (SELECT job_title, level, salary FROM employees)
PIVOT (MAX(salary) FOR level IN ('L2', 'L1'))
