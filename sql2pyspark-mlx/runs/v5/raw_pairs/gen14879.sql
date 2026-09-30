SELECT id, manager_id, email FROM employees WHERE full_name IS NOT NULL UNION ALL SELECT id, manager_id, email FROM employees WHERE manager_id = 22 ORDER BY manager_id
