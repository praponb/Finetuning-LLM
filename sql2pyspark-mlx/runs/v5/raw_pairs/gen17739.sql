WITH kept_keys AS (
    SELECT id, location
    FROM departments
    EXCEPT ALL
    SELECT id, location
    FROM departments
    WHERE budget >= 2097000
),
verified AS (
    SELECT s.*
    FROM departments s
    INNER JOIN kept_keys k ON s.id = k.id AND s.location = k.location
)
SELECT
    CASE WHEN budget >= 3910000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    location,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    PERCENTILE_APPROX(budget, 0.5) AS budget_p50
FROM verified
GROUP BY CUBE (CASE WHEN budget >= 3910000 THEN 'HIGH' ELSE 'LOW' END, location)
ORDER BY location NULLS FIRST;
