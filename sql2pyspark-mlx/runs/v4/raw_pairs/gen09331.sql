SELECT
    emp.id,
    emp.email AS employee_email,
    mgr.job_title AS manager_job_title,
    mgr.is_remote AS manager_is_remote,
    mgr.hire_date AS manager_hire_date
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.is_remote = FALSE AND mgr.job_title IN ('Engineer', 'Intern', 'Designer')
