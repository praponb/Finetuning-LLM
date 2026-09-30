SELECT id, job_title, salary, ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 3) AS salary_share FROM employees WHERE hire_date <= '2025-09-28' ORDER BY salary_share DESC, salary DESC;
