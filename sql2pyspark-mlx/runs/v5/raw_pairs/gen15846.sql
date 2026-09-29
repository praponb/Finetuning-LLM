WITH base AS (
    SELECT
        *,
        LOWER(location) AS location_lower,
        COALESCE(location, 'DIRECT') AS resolved_location
    FROM departments
    WHERE location IS NOT NULL
),
scored AS (
    SELECT
        id,
        dept_name,
        budget,
        location_lower,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
        DENSE_RANK() OVER w_grp AS budget_dense_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY dept_name, location ORDER BY budget NULLS LAST)
),
cleaned AS (
    SELECT
        id,
        dept_name,
        budget,
        location_lower,
        budget_bucket,
        budget_dense_rank,
        COALESCE(dept_name, 'unassigned') AS dept_name_filled
    FROM scored
    WHERE dept_name IS DISTINCT FROM 'Marketing'
),
filtered AS (
    SELECT
        *,
        CEIL(budget_bucket) AS budget_bucket_whole,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled_2
    FROM cleaned
    WHERE budget_dense_rank = 6
),
remaining_keys AS (
    SELECT id, dept_name
    FROM filtered
    INTERSECT ALL
    SELECT id, dept_name
    FROM departments
    WHERE dept_name NOT IN ('Sales', 'Engineering')
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.dept_name = k.dept_name
)
SELECT
    id,
    dept_name,
    budget,
    location_lower,
    budget_bucket,
    budget_bucket_whole,
    dept_name_filled_2
FROM verified
ORDER BY budget_bucket NULLS LAST, budget_bucket_whole;
