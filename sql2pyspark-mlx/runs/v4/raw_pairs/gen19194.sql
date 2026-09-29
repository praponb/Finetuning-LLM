SELECT
    e.hire_date AS employee_hire_date,
    e.dept_id AS employee_dept_id,
    e.level AS employee_level,
    m.hire_date AS manager_hire_date,
    m.level AS manager_level
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.job_title IN ('Director', 'Analyst', 'Engineer')
ORDER BY manager_hire_date ASC
