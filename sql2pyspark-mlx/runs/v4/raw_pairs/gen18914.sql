WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN budget >= 3908000 THEN 'High' WHEN budget >= 675000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        CONCAT_WS('/', location, dept_name) AS location_dept_name_key,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM departments
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(budget, 1, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget,
        LAG(budget, 1, 0) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget
    FROM cleaned
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary < 84000
        )
)
SELECT
    id,
    dept_name,
    budget,
    budget_level,
    location_dept_name_key,
    dept_name_clean,
    prev_budget
FROM not_flagged
ORDER BY budget NULLS FIRST, prev_budget DESC;
