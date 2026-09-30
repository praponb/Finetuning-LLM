SELECT
    d.id,
    d.location,
    COUNT(e.id) employee_count,
    MAX(e.salary) - MIN(e.salary) salary_range,
    COUNT(CASE WHEN e.job_title = 'Analyst' THEN 1 END) analyst_count
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE e.is_remote
GROUP BY d.id, d.location
ORDER BY d.id;
