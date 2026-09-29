SELECT
    emp.id,
    emp.hire_date AS employee_hire_date,
    mgr.hire_date AS manager_hire_date,
    mgr.salary AS manager_salary,
    mgr.email AS manager_email
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
ORDER BY id DESC
LIMIT 20;
