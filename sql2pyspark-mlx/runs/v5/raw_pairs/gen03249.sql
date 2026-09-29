SELECT * FROM (SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, level, salary FROM employees) PIVOT (SUM(salary) FOR level IN ('L3', 'L5'))
