SELECT d.budget, e.job_title, e.full_name
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
ORDER BY e.job_title ASC;
