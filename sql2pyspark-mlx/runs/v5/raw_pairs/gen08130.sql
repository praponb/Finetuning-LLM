WITH department_stats AS (
    SELECT dept_id, MIN(hire_date) AS min_hire_date, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.dept_name,
    ds.min_hire_date,
    ds.salary_range
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.salary_range >= 30;
