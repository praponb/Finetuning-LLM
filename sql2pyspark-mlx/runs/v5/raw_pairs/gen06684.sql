WITH DepartmentStats AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1, MAX(salary) AS max_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.dept_name,
    d.budget,
    D.pct_l1,
    D.max_salary
FROM departments d
INNER JOIN DepartmentStats D ON d.id = D.dept_id
WHERE D.max_salary >= 92000
ORDER BY dept_name DESC, D.max_salary DESC;
