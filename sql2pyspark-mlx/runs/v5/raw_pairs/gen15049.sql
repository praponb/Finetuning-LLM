WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name IS NOT NULL OR dept_name IN ('Engineering', 'Legal')
),
agg AS (
    SELECT location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
    FROM recent_departments
    GROUP BY location
)
SELECT location, location_values
FROM agg
ORDER BY location_values;
