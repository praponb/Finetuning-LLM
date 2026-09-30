WITH cleaned AS (
    SELECT
        *,
        ROUND(budget * 100, 2) AS adjusted_budget,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM departments
    WHERE location IN ('Bangkok', 'New York')
)
SELECT
    location,
    CASE WHEN budget >= 3322000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'New York' THEN 1 END), 0), 2) AS budget_per_new_york,
    APPROX_PERCENTILE(adjusted_budget, ARRAY(0.25, 0.5, 0.75)) AS adjusted_budget_quartiles,
    COUNT(*) FILTER (WHERE location = 'Berlin') AS berlin_rows
FROM cleaned
GROUP BY location, CASE WHEN budget >= 3322000 THEN 'HIGH' ELSE 'LOW' END
ORDER BY berlin_rows
