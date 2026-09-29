SELECT
    d.dept_name,
    d.budget,
    d.location,
    e.salary,
    e.is_remote,
    e.job_title,
    CASE WHEN e.is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
ORDER BY d.budget DESC;
