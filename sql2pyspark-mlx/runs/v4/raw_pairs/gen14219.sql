SELECT job_title, level, AVG(salary) AS avg_salary, SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count FROM employees WHERE dept_id = 13 GROUP BY job_title, level;
