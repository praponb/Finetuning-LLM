WITH DepartmentSummary AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    D.l5_salary,
    D.salary_range
FROM departments d
LEFT JOIN DepartmentSummary D ON d.id = D.dept_id
WHERE D.salary_range > 53
ORDER BY D.l5_salary DESC;
