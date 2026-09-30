SELECT dept_id, COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count, MAX(hire_date) AS latest_hire_date FROM employees GROUP BY dept_id ORDER BY latest_hire_date, dept_id DESC;
