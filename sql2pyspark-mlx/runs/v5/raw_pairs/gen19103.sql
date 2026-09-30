SELECT level, YEAR(hire_date) AS hire_year, MAX(salary) - MIN(salary) AS salary_range, COUNT(*) AS employee_count, MIN(salary) AS min_salary FROM employees WHERE job_title = 'Director' GROUP BY 1, 2;
