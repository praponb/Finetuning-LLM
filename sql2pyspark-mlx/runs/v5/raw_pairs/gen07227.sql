WITH base AS (
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        CASE WHEN location IN ('Berlin', 'Singapore') THEN 'Group A' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location NOT IN ('London', 'Berlin', 'Chiang Mai')
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IS NOT NULL
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (10, 24, 38)
        )
),
scored AS (
    SELECT
        *,
        MIN(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget,
        COUNT(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget
    FROM eligible
)
SELECT
    location,
    dept_name,
    PERCENTILE_APPROX(running_count_budget, ARRAY(0.25, 0.5, 0.75)) AS running_count_budget_quartiles,
    COUNT(*) FILTER (WHERE dept_name = 'Engineering') AS engineering_rows,
    PERCENTILE_APPROX(running_count_budget, 0.95) AS running_count_budget_p95,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SUM(running_avg_budget) FILTER (WHERE dept_name = 'Marketing') AS marketing_total_running_avg_budget,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM scored
GROUP BY location, dept_name;
