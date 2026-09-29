WITH department_stats AS (
    SELECT dept_id, MIN(hire_date) AS first_hire_date, MIN(salary) AS min_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.location,
    ds.first_hire_date,
    ds.min_salary
FROM departments d
JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.min_salary > 145000
ORDER BY d.dept_name, id
LIMIT 5
