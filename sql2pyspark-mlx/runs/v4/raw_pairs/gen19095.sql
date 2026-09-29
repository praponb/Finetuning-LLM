WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id) AS rank_in_group
        FROM departments
    ) ranked
    WHERE rank_in_group = 1
),
base AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        rank_in_group,
        CAST(rank_in_group AS STRING) AS rank_in_group_str
    FROM ranked
    WHERE dept_name IN ('HR', 'Engineering')
),
remaining_keys AS (
    SELECT id, dept_name
    FROM base
    INTERSECT
    SELECT id, dept_name
    FROM departments
    WHERE budget < 2596000
),
verified AS (
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id AND s.dept_name = k.dept_name
)
SELECT
    id,
    dept_name,
    budget,
    rank_in_group,
    rank_in_group_str
FROM verified
ORDER BY dept_name
