WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(location)) location_title
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location NOT IN ('Tokyo', 'Bangkok')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM filtered
) src
PIVOT (
    MAX(budget) AS total,
    COUNT(1) AS item_count
    FOR location IN ('Berlin' AS berlin, 'London' AS london)
)
