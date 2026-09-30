SELECT id, manager_id, job_title FROM employees WHERE salary >= 65000 UNION SELECT id, manager_id, job_title FROM employees WHERE manager_id = 10
