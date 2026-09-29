WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(budget * 100, 2) AS budget_scaled,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM departments
    WHERE budget <= 2370000
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.full_name IS NOT NULL
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_scaled,
        resolved_location,
        MAX(budget) OVER (PARTITION BY location, dept_name) AS location_max_budget,
        FIRST_VALUE(location) IGNORE NULLS OVER (ORDER BY budget_scaled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location
    FROM kept
)
-- Multi-level aggregation
SELECT
    CASE WHEN budget_scaled >= 83 THEN 'LARGE' ELSE 'SMALL' END AS budget_scaled_tier,
    SUM(CASE WHEN dept_name = 'Finance' THEN budget_scaled ELSE 0 END) AS finance_budget_scaled,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(location_max_budget, ARRAY(0.25, 0.5, 0.75)) AS location_max_budget_quartiles
FROM enriched
GROUP BY CASE WHEN budget_scaled >= 83 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY event_count ASC NULLS FIRST
