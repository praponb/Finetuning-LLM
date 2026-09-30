WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title <> 'Engineer'
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(dept_name, 1, 4) AS dept_name_prefix
    FROM eligible
    WHERE dept_name IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date BETWEEN '2024-07-01' AND '2025-05-01'
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        location,
        COALESCE(location, 'unassigned') AS location_filled,
        COALESCE(location, 'N/A') AS location_filled_2
    FROM not_flagged
    WHERE dept_name IS NOT NULL
)
SELECT
    id,
    location,
    location_filled,
    location_filled_2,
    CASE WHEN location RLIKE '^(New York|London)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS location_zone
FROM valid
ORDER BY location DESC NULLS FIRST;
