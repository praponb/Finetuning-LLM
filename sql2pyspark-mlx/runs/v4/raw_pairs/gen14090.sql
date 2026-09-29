WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE location <> 'Bangkok' AND dept_name = 'Sales'
),
summary AS (
    SELECT location, MAX(budget) - MIN(budget) AS budget_range
    FROM recent_departments
    GROUP BY location
)
SELECT location, budget_range
FROM summary
WHERE budget_range >= 51;
