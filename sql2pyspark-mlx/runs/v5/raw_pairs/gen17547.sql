WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM departments
    WHERE dept_name IN ('Finance', 'Marketing', 'Legal')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget_int) AS row_num
    FROM base
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget_int,
        s.dept_name_clean,
        s.row_num,
        x.hire_date,
        x.salary
    FROM ranked s
    JOIN LATERAL (
            SELECT c.hire_date, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date > '2025-02-01'
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(row_num AS DOUBLE) AS row_num_str,
        COALESCE(location, 'NONE') AS location_clean
    FROM not_flagged
    WHERE row_num = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget_int
    FROM filtered
) src
PIVOT (
    AVG(budget_int)
    FOR location IN ('Berlin' AS berlin, 'New York' AS new_york)
);
