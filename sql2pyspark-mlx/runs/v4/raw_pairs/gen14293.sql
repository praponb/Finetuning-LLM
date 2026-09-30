SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE LOWER(TRIM(full_name)) = 'carol diaz')
PIVOT (SUM(salary) FOR level IN ('L3', 'L1'));
