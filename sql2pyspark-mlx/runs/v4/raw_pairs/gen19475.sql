WITH department_summary AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.location,
    d.dept_name,
    ds.avg_salary,
    ds.stddev_salary
FROM departments d
JOIN department_summary ds ON d.id = ds.dept_id
ORDER BY location
