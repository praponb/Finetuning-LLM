WITH valid AS (
    SELECT
        *,
        ROUND(budget * 100, 2) AS budget_scaled,
        SUBSTRING(dept_name, 1, 4) AS dept_name_prefix
    FROM departments
    WHERE dept_name IS NOT NULL
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(location) IGNORE NULLS OVER (ORDER BY budget_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location,
        AVG(budget_scaled) OVER (PARTITION BY dept_name ORDER BY budget_scaled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_scaled_avg_3
    FROM valid
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND MONTH(x.hire_date) IN (5, 7, 10)
        )
),
totals AS (
    SELECT
        dept_name,
        CASE WHEN budget >= 3415000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
        COUNT(*) AS row_count,
        SUM(budget_scaled_avg_3) AS total_budget_scaled_avg_3,
        COUNT(DISTINCT id) AS distinct_ids,
        AVG(budget_scaled) FILTER (WHERE location = 'Tokyo') AS tokyo_avg_budget_scaled,
        GROUPING_ID() AS grouping_id
    FROM not_flagged
    GROUP BY GROUPING SETS ((dept_name, CASE WHEN budget >= 3415000 THEN 'HIGH' ELSE 'LOW' END), (dept_name), ())
)
SELECT
    dept_name,
    row_count,
    distinct_ids,
    tokyo_avg_budget_scaled
FROM totals
ORDER BY distinct_ids DESC, tokyo_avg_budget_scaled
