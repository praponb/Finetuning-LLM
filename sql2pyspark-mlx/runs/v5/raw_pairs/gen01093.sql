WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget,
        MAX(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget,
        MIN(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget
    FROM departments
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email IS NOT NULL
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(running_max_budget, TRUE) OVER (PARTITION BY location, dept_name ORDER BY running_max_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_budget,
        NTILE(4) OVER (PARTITION BY dept_name, location ORDER BY running_min_budget DESC, id) AS running_min_budget_bucket,
        LEAD(running_sum_budget, 1) OVER (ORDER BY running_min_budget, id) AS next_running_sum_budget,
        AVG(running_sum_budget) OVER (ORDER BY running_max_budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_sum_budget_avg_2
    FROM kept
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.running_sum_budget,
        s.running_min_budget,
        x.hire_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.running_sum_budget,
        s.running_min_budget,
        s.hire_date,
        x.employee_job_title
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.salary
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_sum_budget >= 18234000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_budget_tier,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    CASE WHEN salary >= 129000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    SUM(CASE WHEN employee_job_title = 'Manager' THEN running_sum_budget ELSE 0 END) AS manager_running_sum_budget,
    COUNT(CASE WHEN employee_job_title = 'Designer' THEN 1 END) AS designer_count,
    PERCENTILE_APPROX(running_sum_budget, 0.5) AS running_sum_budget_p50
FROM with_last
GROUP BY CASE WHEN running_sum_budget >= 18234000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', hire_date), CASE WHEN salary >= 129000 THEN 'LARGE' ELSE 'SMALL' END
HAVING designer_count >= 1
