SELECT dept_id FROM employees WHERE job_title IS NOT NULL EXCEPT SELECT dept_id FROM employees WHERE email IS NOT NULL ORDER BY dept_id;
