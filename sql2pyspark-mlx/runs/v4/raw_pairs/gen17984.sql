WITH base AS (
    SELECT
        *,
        CASE WHEN budget >= 3301000 THEN 'High' WHEN budget >= 1340000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM departments
    WHERE dept_name IS NOT NULL
)
SELECT
    *,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
    LAG(budget, 1) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS prev_budget
FROM base;
