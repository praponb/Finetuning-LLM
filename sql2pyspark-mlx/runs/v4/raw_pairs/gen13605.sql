WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'DIRECT') AS resolved_dept_name
    FROM departments
    WHERE dept_name = 'Legal'
)
-- Keep one row per key (latest first)
SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget NULLS LAST, id) AS latest_rank
FROM base
QUALIFY latest_rank = 1;
