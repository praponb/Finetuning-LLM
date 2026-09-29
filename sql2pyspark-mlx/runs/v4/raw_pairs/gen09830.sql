SELECT
    d.id,
    d.location,
    d.dept_name,
    d.budget
FROM departments d
WHERE d.budget >= 2833000 AND d.id IN (SELECT dept_id FROM employees WHERE job_title = 'Manager');
