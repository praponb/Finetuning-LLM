WITH department_totals AS (
    SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, dt.stddev_salary
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id;
