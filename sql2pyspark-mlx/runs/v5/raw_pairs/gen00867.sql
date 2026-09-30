WITH department_totals AS (
    SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, SUM(salary) AS total_salary
    FROM employees
    WHERE dept_id IN (7, 14, 24, 25)
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    dt.stddev_salary,
    dt.total_salary
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY dept_name DESC
