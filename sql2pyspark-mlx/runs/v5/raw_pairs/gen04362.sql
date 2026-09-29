SELECT is_remote, level, AVG(salary) AS avg_salary FROM employees GROUP BY is_remote, level HAVING avg_salary > 119000
