SELECT
    emp.id,
    emp.is_remote AS employee_is_remote,
    emp.manager_id AS employee_manager_id,
    mgr.is_remote AS manager_is_remote
FROM employees emp
LEFT OUTER JOIN employees mgr ON emp.manager_id = mgr.id
ORDER BY 3 DESC, id;
