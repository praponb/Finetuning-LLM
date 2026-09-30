WITH DepartmentTotals AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_engineer
    FROM employees
    WHERE dept_id IN (19, 28, 36)
    GROUP BY dept_id
)
SELECT departments.location, DepartmentTotals.pct_engineer
FROM departments
INNER JOIN DepartmentTotals ON departments.id = DepartmentTotals.dept_id
ORDER BY location
