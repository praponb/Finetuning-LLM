SELECT * FROM (SELECT is_remote, level, salary FROM employees)
PIVOT (COUNT(salary) FOR level IN ('L1', 'L5', 'L3'))
ORDER BY is_remote
