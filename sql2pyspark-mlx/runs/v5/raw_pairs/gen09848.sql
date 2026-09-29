SELECT id, dept_id, manager_id, is_remote, UPPER(TRIM(full_name)) AS full_name_normalized, IF(is_remote, 'Yes', 'No') AS is_remote_label FROM employees;
