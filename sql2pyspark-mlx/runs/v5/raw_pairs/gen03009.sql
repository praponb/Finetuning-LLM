SELECT level, QUARTER(hire_date) AS hire_quarter, MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE job_title IN ('Analyst', 'Director') OR level = 'L5'
GROUP BY level, QUARTER(hire_date)
HAVING MAX(salary) - MIN(salary) >= 47;
