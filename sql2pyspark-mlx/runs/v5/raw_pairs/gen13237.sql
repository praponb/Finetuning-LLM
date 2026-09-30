SELECT
    d.location,
    d.budget,
    e.dept_id,
    e.job_title,
    e.hire_date,
    CAST(d.budget AS INT) AS budget_int
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.level NOT IN ('L5', 'L4') AND d.location = 'London'
