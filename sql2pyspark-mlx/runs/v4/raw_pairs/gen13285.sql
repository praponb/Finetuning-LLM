WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget >= 4013000
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title IN ('Analyst', 'Director')
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS budget_cume_dist,
        MIN(is_chiang_mai) OVER (PARTITION BY location) AS location_min_is_chiang_mai
    FROM eligible
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget NULLS FIRST)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        dept_name,
        budget_cume_dist,
        budget
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric IN (budget_cume_dist AS BUDGET_CUME_DIST_VAL, budget)
)
