SELECT
    e.id,
    e.hire_date,
    e.salary,
    e.job_title,
    d.location,
    d.dept_name,
    d.budget,
    COALESCE(d.budget, 0) AS budget_filled
FROM employees e
RIGHT JOIN departments d ON d.id = e.dept_id
ORDER BY e.salary;
