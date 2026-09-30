WITH matched_keys AS (
    SELECT id
    FROM departments
    EXCEPT
    SELECT id
    FROM departments
    WHERE budget < 3483000
),
kept_rows AS (
    SELECT s.*
    FROM departments s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    CASE WHEN budget >= 3327000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    location,
    MAX(budget) FILTER (WHERE dept_name = 'Legal') AS legal_max_budget,
    COUNT_IF(location = 'New York') AS new_york_events,
    COUNT_IF(dept_name = 'Engineering') AS engineering_events,
    GROUPING(CASE WHEN budget >= 3327000 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM kept_rows
GROUP BY GROUPING SETS ((CASE WHEN budget >= 3327000 THEN 'HIGH' ELSE 'LOW' END, location), (CASE WHEN budget >= 3327000 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING legal_max_budget < 0
