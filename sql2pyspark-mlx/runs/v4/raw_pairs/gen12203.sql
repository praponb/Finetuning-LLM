WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('/', location, dept_name) location_dept_name_key
    FROM departments
    WHERE budget < 2063000
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (4, 5, 32, 33)
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary <= 108000
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget >= 3936000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    location,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SUM(budget) FILTER (WHERE dept_name = 'Sales') AS sales_total_budget
FROM not_flagged
GROUP BY CASE WHEN budget >= 3936000 THEN 'HIGH' ELSE 'LOW' END, location;
