WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(budget, 2) AS budget_rounded,
        FLOOR(budget) AS budget_whole,
        COALESCE(location, 'N/A') AS location_clean
    FROM departments
    WHERE location IS DISTINCT FROM 'Bangkok'
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_name
    FROM filtered
    EXCEPT DISTINCT
    SELECT dept_name
    FROM departments
    WHERE dept_name IN ('Legal', 'Marketing', 'Engineering')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id = 33
        )
),
eligible_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id = 13
        )
)
SELECT
    dept_name,
    location,
    budget_rounded,
    budget_whole,
    CASE WHEN location RLIKE '^(Singapore|London)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS location_zone
FROM eligible_departments
