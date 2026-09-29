WITH department_totals AS (
    SELECT dept_id, MAX(hire_date) AS latest_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.location, dt.latest_hire_date
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id;
