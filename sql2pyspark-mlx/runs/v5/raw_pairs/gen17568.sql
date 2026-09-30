WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name IN ('Sales', 'Engineering')
),
summary AS (
    SELECT location, AVG(budget) AS avg_budget, SUM(budget) AS total_budget
    FROM valid_departments
    GROUP BY location
)
SELECT *
FROM summary
ORDER BY total_budget ASC;
