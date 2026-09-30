SELECT * FROM (SELECT level, job_title, salary FROM employees WHERE level IS NULL) PIVOT (MAX(salary) FOR job_title IN ('Engineer', 'Manager')) ORDER BY level
