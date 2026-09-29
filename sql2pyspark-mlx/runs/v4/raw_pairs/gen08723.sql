WITH department_totals AS (
    SELECT dept_id, MAX(salary) AS max_salary
    FROM employees
    WHERE LOWER(TRIM(full_name)) = 'mia rossi'
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    d.dept_name,
    dt.max_salary
FROM departments d
LEFT OUTER JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY d.id
