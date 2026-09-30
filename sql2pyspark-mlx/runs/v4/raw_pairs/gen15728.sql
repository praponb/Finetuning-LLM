SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE job_title != 'Engineer')
PIVOT (MAX(salary) FOR level IN ('L3', 'L5', 'L1'))
ORDER BY job_title
