SELECT salary, DAYOFWEEK(hire_date) AS hire_date_dayofweek, ROUND(salary, 1) AS salary_rounded FROM employees WHERE level IS NOT NULL AND manager_id = 1
