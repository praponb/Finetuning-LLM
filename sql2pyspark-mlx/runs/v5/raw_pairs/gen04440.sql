WITH department_totals AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, dt.salary_range
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.salary_range > 49;
