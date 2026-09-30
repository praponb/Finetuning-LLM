SELECT
    departments.budget,
    departments.dept_name,
    departments.location,
    employees.job_title
FROM departments
LEFT JOIN employees ON employees.dept_id = departments.id
WHERE employees.manager_id = 26
ORDER BY budget DESC;
