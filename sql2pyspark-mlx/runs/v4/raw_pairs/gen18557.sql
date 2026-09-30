SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.job_title AS employee_job_title,
    m.manager_id AS manager_manager_id,
    m.dept_id AS manager_dept_id,
    CASE WHEN m.salary >= 163000 THEN 'High' WHEN m.salary >= 59000 THEN 'Medium' ELSE 'Low' END AS salary_tier
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE NOT (e.salary > 121000 AND m.job_title IN ('Designer', 'Director'))
ORDER BY 4 DESC;
