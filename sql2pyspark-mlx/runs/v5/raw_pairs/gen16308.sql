WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(location) AS location_upper,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name IN ('Legal', 'Finance', 'HR')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT location, id
    FROM valid
    INTERSECT
    SELECT location, id
    FROM departments
    WHERE location <> 'Singapore'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.location = k.location AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        dept_name,
        budget,
        location_upper,
        resolved_dept_name,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY dept_name_length DESC NULLS LAST) AS dept_name_length_dense_rank,
        DENSE_RANK() OVER (ORDER BY budget) AS budget_dense_rank
    FROM kept_rows
),
enriched_departments AS (
    -- Window metrics over several different partitions
    SELECT
        dept_name,
        resolved_dept_name,
        ROUND(budget_dense_rank / SUM(budget_dense_rank) OVER (PARTITION BY dept_name), 4) AS budget_dense_rank_share,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST) AS budget_cume_dist
    FROM enriched
    WHERE dept_name = 'Sales'
)
SELECT
    dept_name,
    resolved_dept_name,
    budget_dense_rank_share,
    budget_cume_dist
FROM enriched_departments
