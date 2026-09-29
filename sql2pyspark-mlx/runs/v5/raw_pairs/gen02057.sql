WITH department_summary AS (
    SELECT dept_id, COUNT(DISTINCT salary) unique_managers, COUNT(DISTINCT salary) unique_depts
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    ds.unique_managers,
    ds.unique_depts
FROM departments d
JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.unique_depts > 6
ORDER BY unique_managers DESC;
