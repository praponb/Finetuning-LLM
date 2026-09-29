WITH DepartmentTotals AS (
    SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.location, D.stddev_salary
FROM departments d
INNER JOIN DepartmentTotals D ON d.id = D.dept_id
