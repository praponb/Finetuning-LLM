SELECT id, manager_id FROM employees WHERE full_name LIKE '%Brown%' EXCEPT SELECT id, manager_id FROM employees WHERE MONTH(hire_date) IN (2, 5, 6)
