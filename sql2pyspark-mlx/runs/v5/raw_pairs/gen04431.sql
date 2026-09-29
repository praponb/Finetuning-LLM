SELECT
    emp.is_remote AS employee_is_remote,
    mgr.manager_id AS manager_manager_id,
    mgr.email AS manager_email,
    mgr.level AS manager_level,
    TRIM(emp.level) AS clean_level
FROM employees emp
LEFT OUTER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.level = 'L5'
ORDER BY manager_level, 5 DESC
