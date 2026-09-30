SELECT
    d.location,
    d.dept_name,
    e.dept_id,
    CAST(e.is_remote AS INT) AS is_remote_flag
FROM departments d
JOIN employees e ON d.id = e.dept_id
WHERE d.location = 'Chiang Mai' AND e.job_title = 'Analyst';
