WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'unassigned') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
      AND budget BETWEEN 1385000 AND 2717000
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level = 'L5'
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(location) AS clean_location,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled_2
    FROM eligible
    WHERE location NOT IN ('Bangkok', 'London')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
filtered_departments AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(budget) AS budget_whole
    FROM ranked
    WHERE location IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN location IN ('Tokyo', 'London') THEN 'Priority' ELSE 'Other' END AS location_group
    FROM filtered_departments
    WHERE dedup_rank < 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN budget_filled >= 72 THEN 'HIGH' ELSE 'LOW' END AS budget_filled_tier,
    COUNT_IF(location = 'Singapore') AS singapore_events,
    COUNT_IF(location = 'London') AS london_events,
    COUNT(DISTINCT dept_name) AS distinct_dept_names
FROM cleaned
GROUP BY CASE WHEN budget_filled >= 72 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT_IF(location = 'Singapore') >= 1
ORDER BY london_events DESC
