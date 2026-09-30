WITH department_stats AS (
    SELECT dept_id, MIN(hire_date) first_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.dept_name, ds.first_hire_date
FROM departments d
LEFT JOIN department_stats ds ON d.id = ds.dept_id
ORDER BY dept_name;
