WITH metrics AS (
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 4) budget_share,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_dept_name,
        MIN(budget) OVER (PARTITION BY location) location_min_budget
    FROM departments
    WHERE dept_name = 'Finance'
)
SELECT
    dept_name,
    SUM(CASE WHEN location = 'Berlin' THEN budget_share ELSE 0 END) AS berlin_budget_share,
    SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget,
    SIZE(COLLECT_SET(location)) AS num_locations,
    SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) AS engineering_budget
FROM metrics
GROUP BY dept_name;
