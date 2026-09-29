WITH department_stats AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, ds.avg_salary
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.avg_salary > 42
ORDER BY avg_salary
