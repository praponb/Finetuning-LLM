WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN dept_name IN ('HR', 'Finance') THEN 'Priority' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE location IS NOT DISTINCT FROM 'Singapore'
),
valid_departments AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM valid
    WHERE dept_name = 'Finance'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        budget,
        dept_name_group,
        CAST(budget AS INT) AS budget_int,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM valid_departments
    WHERE dept_name IS NOT NULL
      AND dept_name IS DISTINCT FROM 'HR'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IN (24, 28, 36)
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.full_name IS NULL
        )
)
SELECT
    id,
    dept_name,
    budget,
    budget_int
FROM not_flagged
