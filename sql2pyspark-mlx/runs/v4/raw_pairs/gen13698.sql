SELECT emp.id, emp.hire_date AS employee_hire_date, mgr.salary AS manager_salary
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.dept_id IN (1, 2, 20, 24)
ORDER BY id DESC
LIMIT 10
