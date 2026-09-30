WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(dept_name, 1, 3) AS dept_name_prefix,
        CONCAT(dept_name, ' ', location) AS dept_name_location,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM departments
    WHERE budget < 2399000
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_prefix,
        location_clean,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM valid
    WHERE dept_name IS NOT NULL
      AND location IN ('London', 'New York', 'Chiang Mai')
),
valid_departments AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM cleaned
    WHERE location IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary BETWEEN 112000 AND 122000
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget_filled
    FROM not_flagged
) src
PIVOT (
    MAX(budget_filled) AS total,
    COUNT(1) AS cnt
    FOR location IN ('London' AS london, 'Chiang Mai' AS chiang_mai, 'Singapore' AS singapore)
);
