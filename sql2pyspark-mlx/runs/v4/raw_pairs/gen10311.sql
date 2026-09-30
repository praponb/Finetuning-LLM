SELECT
    emp.id,
    emp.manager_id AS employee_manager_id,
    emp.salary AS employee_salary,
    mgr.manager_id AS manager_manager_id,
    mgr.level AS manager_level,
    mgr.salary AS manager_salary
FROM employees emp
JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.job_title != 'Engineer';
