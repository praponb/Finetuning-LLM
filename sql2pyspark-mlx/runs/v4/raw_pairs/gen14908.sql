SELECT s.*, latest.job_title FROM departments s LEFT JOIN LATERAL (SELECT c.job_title FROM employees c WHERE c.dept_id = s.id ORDER BY c.salary DESC, c.id DESC LIMIT 1) latest ON TRUE
