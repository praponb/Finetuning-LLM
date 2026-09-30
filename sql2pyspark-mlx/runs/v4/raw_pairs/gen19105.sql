SELECT level, MIN(salary) AS min_salary FROM employees WHERE email NOT LIKE '%@company.co' GROUP BY level
