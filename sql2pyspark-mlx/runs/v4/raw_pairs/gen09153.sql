WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM departments
    WHERE budget BETWEEN 1413000 AND 3366000
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title IN ('Designer', 'Analyst')
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        dept_name_filled,
        GREATEST(budget_filled, budget) AS greatest_budget_filled_budget,
        TRIM(location) AS clean_location
    FROM not_flagged
    WHERE budget BETWEEN 1312000 AND 3987000
),
cleaned_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(greatest_budget_filled_budget AS INT) AS greatest_budget_filled_budget_int,
        CONCAT_WS(',', location, dept_name) AS location_dept_name_key,
        COALESCE(location, 'N/A') AS location_clean
    FROM cleaned
    WHERE location IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        greatest_budget_filled_budget
    FROM cleaned_departments
) src
PIVOT (
    SUM(greatest_budget_filled_budget)
    FOR location IN ('London' AS london, 'Bangkok' AS bangkok)
)
