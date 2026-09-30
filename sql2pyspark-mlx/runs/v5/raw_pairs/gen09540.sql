SELECT level, dept_id, unique_job_titles FROM (SELECT level, dept_id, COUNT(DISTINCT salary) AS unique_job_titles FROM employees GROUP BY level, dept_id) AS sub WHERE unique_job_titles >= 6;
