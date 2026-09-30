SELECT d.dept_name, e.job_title, e.manager_id
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE d.location = 'Berlin'
ORDER BY manager_id DESC, dept_name;
