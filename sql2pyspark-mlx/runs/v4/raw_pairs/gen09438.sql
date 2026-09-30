WITH ranked AS (
    SELECT *
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) = 1
)
SELECT
    location,
    CASE WHEN budget >= 3760000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'Singapore' THEN 1 END), 0), 2) AS budget_per_singapore
FROM ranked
GROUP BY location, CASE WHEN budget >= 3760000 THEN 'HIGH' ELSE 'LOW' END
ORDER BY budget_tier DESC NULLS LAST
