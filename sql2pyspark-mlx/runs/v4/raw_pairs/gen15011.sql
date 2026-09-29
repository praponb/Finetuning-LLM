SELECT *, IF(is_remote, 'Yes', 'No') AS is_remote_label
FROM employees
WHERE manager_id IS NOT NULL;
