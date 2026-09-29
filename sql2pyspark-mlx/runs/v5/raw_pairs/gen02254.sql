SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE level IS NOT NULL) PIVOT (AVG(salary) FOR level IN ('L3', 'L2', 'L4')) ORDER BY job_title;
