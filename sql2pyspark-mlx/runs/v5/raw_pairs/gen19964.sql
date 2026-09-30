WITH department_stats AS (
    SELECT dept_id, SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END) AS designer_count
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    departments.location,
    department_stats.designer_count
FROM departments
JOIN department_stats ON departments.id = department_stats.dept_id
WHERE department_stats.designer_count > 71
