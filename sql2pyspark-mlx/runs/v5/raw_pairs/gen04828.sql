WITH DepartmentTotals AS (
    SELECT dept_id, SUM(salary) total_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.location,
    D.total_salary
FROM departments d
LEFT JOIN DepartmentTotals D ON d.id = D.dept_id
WHERE D.total_salary >= 289000
ORDER BY d.budget DESC, total_salary DESC
