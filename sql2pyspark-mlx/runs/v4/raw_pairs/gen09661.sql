SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE level NOT IN ('L4', 'L1'))
PIVOT (MAX(salary) FOR level IN ('L5', 'L2'))
ORDER BY job_title;
