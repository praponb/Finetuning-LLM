SELECT * FROM (SELECT YEAR(hire_date) AS hire_year, level, salary FROM employees WHERE manager_id IS NOT NULL) PIVOT (MAX(salary) FOR level IN ('L5', 'L2')) ORDER BY hire_year
