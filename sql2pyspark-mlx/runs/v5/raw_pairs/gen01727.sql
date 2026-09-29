SELECT * FROM (SELECT is_remote, level, salary FROM employees) PIVOT (MAX(salary) FOR level IN ('L2', 'L3')) ORDER BY is_remote
