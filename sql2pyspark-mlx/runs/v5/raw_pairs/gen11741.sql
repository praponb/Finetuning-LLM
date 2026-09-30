WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', location, dept_name) AS location_dept_name_key,
        COALESCE(location, 'DIRECT') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM valid
) src
PIVOT (
    MAX(budget) AS total
    FOR location IN ('Singapore' AS singapore, 'Berlin' AS berlin)
);
