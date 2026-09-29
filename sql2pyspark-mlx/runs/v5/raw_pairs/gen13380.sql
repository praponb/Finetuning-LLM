SELECT
    emp.id,
    emp.hire_date AS employee_hire_date,
    emp.level AS employee_level,
    mgr.level AS manager_level,
    mgr.is_remote AS manager_is_remote,
    mgr.job_title AS manager_job_title,
    COALESCE(mgr.level, 'N/A') AS level_filled
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.email LIKE '%@startup.io' OR mgr.full_name LIKE 'Ivy%';
