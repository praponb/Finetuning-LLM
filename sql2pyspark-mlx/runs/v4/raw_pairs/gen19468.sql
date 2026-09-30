WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn
    FROM departments
    QUALIFY rn = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget NULLS LAST, id) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        budget,
        rn,
        UPPER(dept_name) AS dept_name_upper,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM deduped
    WHERE location IS NOT NULL
      AND dept_name = 'Legal'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT dept_name, id
    FROM filtered
    INTERSECT
    SELECT dept_name, id
    FROM departments
    WHERE location = 'Singapore'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.id = k.id
)
-- Window metrics over several different partitions
SELECT
    *,
    CUME_DIST() OVER (ORDER BY budget NULLS FIRST) AS budget_cume_dist,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
    STDDEV_SAMP(budget) OVER (PARTITION BY location, dept_name) AS location_stddev_samp_budget
FROM verified
