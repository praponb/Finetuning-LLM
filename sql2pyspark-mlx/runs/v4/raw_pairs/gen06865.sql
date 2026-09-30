SELECT emp.level AS employee_level, mgr.is_remote AS manager_is_remote, mgr.full_name AS manager_full_name
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.manager_id IS NOT NULL AND emp.hire_date BETWEEN '2024-03-01' AND '2025-06-10'
