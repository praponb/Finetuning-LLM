SELECT id, dept_id, hire_date, job_title, DATE_FORMAT(hire_date, 'yyyy-MM-dd') AS hire_date_label FROM employees WHERE dept_id IN (3, 13, 22, 29) OR is_remote
