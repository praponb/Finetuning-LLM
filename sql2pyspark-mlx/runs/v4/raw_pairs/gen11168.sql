SELECT id, job_title FROM employees INTERSECT SELECT id, job_title FROM employees WHERE full_name LIKE '%Lee%'
