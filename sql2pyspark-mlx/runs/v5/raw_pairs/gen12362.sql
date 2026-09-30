SELECT id, full_name, job_title FROM employees WHERE UPPER(full_name) LIKE '%RAC%' ORDER BY full_name DESC, job_title DESC;
