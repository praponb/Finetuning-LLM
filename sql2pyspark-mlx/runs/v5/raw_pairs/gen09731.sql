WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        LENGTH(dept_name) AS dept_name_length
    FROM departments
    WHERE location IS NOT NULL
      AND budget <= 2397000
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget,
        dept_name_lower,
        MAX(dept_name_length) OVER (PARTITION BY location, dept_name ORDER BY dept_name_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_dept_name_length,
        MIN(dept_name_length) OVER (PARTITION BY location, dept_name ORDER BY dept_name_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_dept_name_length,
        SUM(dept_name_length) OVER (PARTITION BY location, dept_name ORDER BY dept_name_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_dept_name_length
    FROM base
    WHERE dept_name_length BETWEEN 23 AND 61
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN location IN ('Tokyo', 'Berlin') THEN 'Priority' ELSE 'Other' END AS location_group,
        COALESCE(location, 'N/A') AS location_filled
    FROM metrics
    WHERE location IS DISTINCT FROM 'London'
      AND last7_sum_dept_name_length < 200
),
base_departments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 2) AS budget_rounded,
        CEIL(last7_sum_dept_name_length) AS last7_sum_dept_name_length_whole
    FROM valid
    WHERE last7_max_dept_name_length >= 26
),
kept_keys AS (
    -- Set intersection on keys
    SELECT location, id
    FROM base_departments
    INTERSECT
    SELECT location, id
    FROM departments
    WHERE location = 'Berlin'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_departments s
    INNER JOIN kept_keys k ON s.location = k.location AND s.id = k.id
)
SELECT
    id,
    location,
    dept_name_lower,
    last7_max_dept_name_length,
    last7_sum_dept_name_length,
    location_group,
    location_filled,
    last7_sum_dept_name_length_whole,
    CASE WHEN location RLIKE '^(Tokyo|Bangkok)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS location_zone
FROM verified
ORDER BY last7_sum_dept_name_length DESC NULLS LAST
