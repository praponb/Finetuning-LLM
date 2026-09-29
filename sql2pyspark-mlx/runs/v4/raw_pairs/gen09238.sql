WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(location) AS location_upper,
        COALESCE(location, 'OTHER') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location = 'Singapore'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id IS NOT NULL
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS latest_rank
        FROM not_flagged
    ) t
    WHERE latest_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE budget < 1945000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share,
    SUM(budget) OVER w_grp AS location_sum_budget,
    RANK() OVER w_ordered AS budget_rank
FROM verified
WINDOW w_grp AS (PARTITION BY location, dept_name), w_ordered AS (ORDER BY budget NULLS LAST)
