SELECT
    d.id,
    d.location,
    d.dept_name,
    t.latest_hire_date
FROM departments d
LEFT OUTER JOIN (SELECT dept_id, MAX(hire_date) AS latest_hire_date
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
