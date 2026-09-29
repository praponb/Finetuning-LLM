WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(location) AS clean_location,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IN (20, 31)
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(budget, 2) OVER w_ordered AS prev_budget,
        LAST_VALUE(budget) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget,
        LAST_VALUE(budget) IGNORE NULLS OVER w_grp AS last_known_budget_2
    FROM eligible
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget, id), w_grp AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        last_known_budget
    FROM metrics
) src
PIVOT (
    AVG(last_known_budget) AS value_sum,
    COUNT(1) AS n
    FOR dept_name IN ('Engineering', 'Marketing')
);
