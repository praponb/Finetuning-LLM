SELECT
    emp.id,
    emp.manager_id AS employee_manager_id,
    emp.hire_date AS employee_hire_date,
    emp.email AS employee_email,
    mgr.job_title AS manager_job_title,
    mgr.level AS manager_level,
    mgr.hire_date AS manager_hire_date
FROM employees AS emp
LEFT JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE emp.manager_id IS NULL AND UPPER(TRIM(mgr.email)) = 'CAROL@COMPANY.CO'
ORDER BY mgr.hire_date;
