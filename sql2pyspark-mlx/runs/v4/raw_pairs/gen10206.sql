WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name = 'Sales'
),
summary AS (
    SELECT dept_name, AVG(budget) AS avg_budget, SUM(budget) AS total_budget
    FROM recent_departments
    GROUP BY dept_name
)
SELECT *
FROM summary;
