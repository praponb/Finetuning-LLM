SELECT
    d.budget,
    d.dept_name,
    d.location,
    e.full_name
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.job_title != 'Analyst' OR e.level NOT IN ('L4', 'L2', 'L1')
