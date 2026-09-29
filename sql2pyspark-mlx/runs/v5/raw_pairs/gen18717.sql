WITH with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.level
    FROM departments s
    JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        budget,
        UPPER(dept_name) AS dept_name_upper
    FROM with_latest
    WHERE level IS DISTINCT FROM 'L4'
      AND budget >= 2613000
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        MAX(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget,
        COUNT(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget
    FROM filtered
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_max_budget) OVER (PARTITION BY dept_name ORDER BY running_avg_budget, id) AS prev_running_max_budget,
        LEAD(running_avg_budget) OVER (PARTITION BY dept_name ORDER BY running_avg_budget, id) AS next_running_avg_budget,
        LAG(running_avg_budget) OVER (PARTITION BY dept_name ORDER BY running_avg_budget, id) AS prev_running_avg_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY running_avg_budget, id) AS row_seq,
        AVG(running_sum_budget) OVER (PARTITION BY dept_name ORDER BY running_sum_budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_sum_budget_avg_5
    FROM with_windows
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND MONTH(x.hire_date) IN (9, 11, 12)
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_running_max_budget >= 3496000 THEN 'HIGH' ELSE 'LOW' END AS prev_running_max_budget_tier,
    SUM(CASE WHEN dept_name = 'Sales' THEN row_seq ELSE 0 END) AS sales_row_seq,
    ROUND(SUM(running_sum_budget_avg_5) / NULLIF(COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END), 0), 2) AS running_sum_budget_avg_5_per_finance,
    COUNT(*) FILTER (WHERE dept_name = 'Marketing') AS marketing_rows,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    ROUND(SUM(running_max_budget) / NULLIF(COUNT(CASE WHEN dept_name = 'HR' THEN 1 END), 0), 2) AS running_max_budget_per_hr,
    SUM(CASE WHEN dept_name = 'Engineering' THEN running_count_budget ELSE 0 END) AS engineering_running_count_budget,
    GROUPING(CASE WHEN prev_running_max_budget >= 3496000 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM eligible
GROUP BY ROLLUP (CASE WHEN prev_running_max_budget >= 3496000 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY sales_row_seq DESC;
