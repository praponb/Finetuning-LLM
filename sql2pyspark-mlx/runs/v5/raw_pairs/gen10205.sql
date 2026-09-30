SELECT id, is_remote, full_name, job_title FROM employees WHERE (manager_id IS NOT NULL AND UPPER(full_name) LIKE '%ENR%') OR manager_id IN (2, 13) ORDER BY 1 DESC, full_name LIMIT 10;
