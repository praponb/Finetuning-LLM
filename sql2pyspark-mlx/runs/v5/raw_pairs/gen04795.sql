WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(location, 1, 2) AS location_prefix
    FROM departments
    WHERE dept_name IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND YEAR(c.hire_date) = 2024
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_name,
        budget,
        location_prefix
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
        DENSE_RANK() OVER w_ordered AS budget_dense_rank,
        LEAD(budget, 1) OVER w_hist AS next_budget,
        LAG(budget, 2, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS prev_budget
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY budget NULLS LAST), w_hist AS (ORDER BY budget, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(budget_dense_rank) AS budget_dense_rank_whole
    FROM with_windows
    WHERE budget_avg_2 = 3006811
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_hire_date
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        budget_dense_rank,
        prev_budget,
        budget_dense_rank_whole,
        employee_hire_date,
        prev_budget * budget AS prev_budget_times_budget
    FROM with_latest
    WHERE budget_dense_rank = 4
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_budget >= 4130000 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_tier,
        CASE WHEN prev_budget_times_budget >= 77 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_times_budget_tier,
        SUM(budget_dense_rank_whole) AS total_budget_dense_rank_whole,
        PERCENTILE_APPROX(budget_dense_rank_whole, 0.9) AS budget_dense_rank_whole_p90,
        APPROX_PERCENTILE(prev_budget_times_budget, ARRAY(0.25, 0.5, 0.75)) AS prev_budget_times_budget_quartiles,
        RANK() OVER (PARTITION BY CASE WHEN prev_budget >= 4130000 THEN 'HIGH' ELSE 'LOW' END ORDER BY SUM(budget_dense_rank_whole) DESC) AS total_budget_dense_rank_whole_rank
    FROM cleaned
    GROUP BY CASE WHEN prev_budget >= 4130000 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_budget_times_budget >= 77 THEN 'HIGH' ELSE 'LOW' END
    HAVING budget_dense_rank_whole_p90 > 0
)
SELECT
    prev_budget_tier,
    prev_budget_times_budget_tier,
    budget_dense_rank_whole_p90,
    prev_budget_times_budget_quartiles,
    total_budget_dense_rank_whole_rank
FROM totals;
