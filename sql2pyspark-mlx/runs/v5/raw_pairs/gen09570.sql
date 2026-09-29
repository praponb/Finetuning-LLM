SELECT
    emp.id,
    emp.job_title AS employee_job_title,
    mgr.manager_id AS manager_manager_id,
    mgr.dept_id AS manager_dept_id,
    mgr.level AS manager_level
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.level IN ('L5', 'L2', 'L4');
