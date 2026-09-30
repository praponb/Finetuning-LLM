SELECT emp.dept_id AS employee_dept_id, mgr.dept_id AS manager_dept_id FROM employees emp LEFT JOIN employees mgr ON emp.manager_id = mgr.id ORDER BY emp.dept_id DESC
