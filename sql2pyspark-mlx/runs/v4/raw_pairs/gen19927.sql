SELECT
    d.location,
    d.dept_name,
    d.budget,
    e.level,
    e.manager_id,
    e.hire_date
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE e.job_title IN ('Director', 'Manager') OR e.job_title IN ('Engineer', 'Intern');
