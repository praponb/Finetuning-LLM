WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, dept_name
    FROM departments
    EXCEPT
    SELECT id, dept_name
    FROM departments
    WHERE dept_name NOT IN ('HR', 'Sales')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.dept_name = k.dept_name
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share_2,
        STDDEV_SAMP(budget) OVER w_part AS location_stddev_samp_budget
    FROM verified
    WINDOW w_part AS (PARTITION BY location)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        budget_share,
        budget_share_2,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(location, 'NONE') AS resolved_location
    FROM enriched
    WHERE location IN ('Bangkok', 'Chiang Mai')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.full_name LIKE 'I%'
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id IS NOT NULL
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget_filled >= 61 THEN 'HIGH' ELSE 'LOW' END AS budget_filled_tier,
    location,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    SUM(budget_share) FILTER (WHERE dept_name = 'Sales') AS sales_total_budget_share
FROM not_flagged
GROUP BY budget_filled_tier, location
HAVING SUM(budget_share) FILTER (WHERE dept_name = 'Sales') >= 0
ORDER BY budget_filled_tier DESC
