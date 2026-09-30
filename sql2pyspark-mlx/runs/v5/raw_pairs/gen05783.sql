WITH department_totals AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary, SUM(salary) AS total_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.location,
    dt.l2_salary,
    dt.total_salary
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id;
