SELECT d.location, e.is_remote, e.job_title
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
ORDER BY job_title DESC, location
