WITH DepartmentStats AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    WHERE dept_id IN (27, 32, 33)
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    D.avg_salary,
    D.salary_range
FROM departments d
INNER JOIN DepartmentStats D ON d.id = D.dept_id
ORDER BY d.budget, id
LIMIT 10;
