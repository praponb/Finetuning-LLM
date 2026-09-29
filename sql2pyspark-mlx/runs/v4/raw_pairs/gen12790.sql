SELECT d.dept_name, e.email, e.job_title
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
ORDER BY job_title ASC
