SELECT job_title, SUM(salary) AS total_salary, MAX(hire_date) AS latest_hire_date FROM employees GROUP BY job_title ORDER BY latest_hire_date, job_title;
