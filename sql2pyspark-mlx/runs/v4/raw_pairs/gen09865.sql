SELECT id, manager_id, level, full_name, IF(is_remote, 'Yes', 'No') is_remote_label FROM employees WHERE hire_date BETWEEN '2023-08-01' AND '2024-09-15' OR job_title IN ('Intern', 'Designer');
