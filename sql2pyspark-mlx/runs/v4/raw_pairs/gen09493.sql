SELECT * FROM (SELECT level, job_title, salary FROM employees) PIVOT (COUNT(salary) FOR job_title IN ('Engineer', 'Designer')) ORDER BY level;
