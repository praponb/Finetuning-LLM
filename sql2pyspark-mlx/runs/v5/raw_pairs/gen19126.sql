SELECT
    d.location,
    e.email,
    e.hire_date,
    e.job_title,
    ROUND(d.budget, 1) AS budget_rounded
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE NOT e.is_remote AND e.manager_id IS NOT NULL;
