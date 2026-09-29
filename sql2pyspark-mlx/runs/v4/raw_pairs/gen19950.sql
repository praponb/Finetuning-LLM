SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE salary BETWEEN 76000 AND 90000)
PIVOT (COUNT(salary) FOR level IN ('L4', 'L1'))
ORDER BY job_title
