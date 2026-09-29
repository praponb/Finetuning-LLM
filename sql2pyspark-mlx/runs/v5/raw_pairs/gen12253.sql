SELECT
    d.id,
    d.location,
    t.max_salary,
    t.manager_count
FROM departments d
INNER JOIN (SELECT dept_id, MAX(salary) AS max_salary, COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count
    FROM employees
    WHERE dept_id IN (7, 8, 21, 32)
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.max_salary >= 143000
ORDER BY manager_count DESC
