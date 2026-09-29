WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.is_remote
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM kept
    WHERE dept_name IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= '2024-09-15'
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_budget,
        MIN(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_budget,
        LEAD(budget, 1) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS next_budget
    FROM eligible
),
eligible_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id IS NULL
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id = 4
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        dept_name,
        CASE WHEN last7_min_budget >= 4040000 THEN 'HIGH' ELSE 'LOW' END AS last7_min_budget_tier,
        COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count,
        SUM(CASE WHEN location = 'Bangkok' THEN last7_avg_budget ELSE 0 END) AS bangkok_last7_avg_budget,
        ROUND(AVG(next_budget), 2) AS avg_next_budget,
        COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows,
        ROUND(AVG(next_budget), 2) AS avg_next_budget_2
    FROM not_flagged
    GROUP BY GROUPING SETS ((dept_name, CASE WHEN last7_min_budget >= 4040000 THEN 'HIGH' ELSE 'LOW' END), (dept_name), ())
)
SELECT
    dept_name,
    last7_min_budget_tier,
    sales_count,
    avg_next_budget,
    hr_rows,
    avg_next_budget_2
FROM totals
ORDER BY sales_count DESC, last7_min_budget_tier DESC;
