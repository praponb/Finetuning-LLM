WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT dept_name, location
    FROM departments
    INTERSECT ALL
    SELECT dept_name, location
    FROM departments
    WHERE location <> 'New York'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.location = k.location
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN budget >= 3565000 THEN 'High' WHEN budget >= 1562000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        SUBSTRING(location, 1, 4) AS location_prefix,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled
    FROM verified
    WHERE location = 'Tokyo'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(budget) OVER (PARTITION BY location, dept_name) AS location_sum_budget,
        RANK() OVER (PARTITION BY dept_name, location ORDER BY budget ASC NULLS FIRST) AS budget_rank,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST) AS budget_dense_rank
    FROM valid
)
SELECT
    id,
    dept_name,
    location,
    budget,
    location_prefix,
    dept_name_filled,
    budget_dense_rank
FROM enriched
