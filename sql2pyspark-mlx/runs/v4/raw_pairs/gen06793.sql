WITH remaining_keys AS (
    -- Set difference on keys
    SELECT location, id
    FROM departments
    EXCEPT
    SELECT location, id
    FROM departments
    WHERE location <> 'Tokyo'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN remaining_keys k ON s.location = k.location AND s.id = k.id
)
SELECT
    dept_name,
    location,
    CASE WHEN location RLIKE '^(London|Tokyo)$' THEN 'GROUP_B' WHEN location RLIKE '^(Bangkok|Chiang Mai)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS location_zone
FROM verified
ORDER BY location DESC, dept_name DESC;
