WITH DepartmentTotals AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_levels, COUNT(*) AS cnt
    FROM employees
    WHERE dept_id IN (3, 16, 26, 31)
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    D.unique_levels,
    D.cnt
FROM departments d
INNER JOIN DepartmentTotals D ON d.id = D.dept_id;
