SELECT * FROM (SELECT job_title, level, salary FROM employees)
PIVOT (MAX(salary) FOR level IN ('L5', 'L4', 'L1', 'L3'));
