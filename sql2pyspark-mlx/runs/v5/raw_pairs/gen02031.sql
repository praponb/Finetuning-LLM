WITH department_summary AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range, MIN(salary) AS min_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.location,
    d.dept_name,
    ds.salary_range,
    ds.min_salary
FROM departments d
LEFT OUTER JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.min_salary > 119000
ORDER BY location, min_salary DESC
