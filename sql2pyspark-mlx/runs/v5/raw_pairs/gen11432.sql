SELECT
    emp.is_remote AS employee_is_remote,
    emp.hire_date AS employee_hire_date,
    mgr.hire_date AS manager_hire_date,
    mgr.salary AS manager_salary,
    mgr.level AS manager_level
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.job_title <> 'Analyst' AND emp.is_remote
ORDER BY manager_level DESC;
