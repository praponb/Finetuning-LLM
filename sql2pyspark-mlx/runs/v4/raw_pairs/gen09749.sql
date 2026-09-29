WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 2) AS budget_rounded,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND NOT ch.is_remote
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        location,
        budget_rounded,
        dept_name_filled,
        CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END AS is_marketing
    FROM eligible
    WHERE dept_name = 'Sales'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget_rounded,
        dept_name_filled,
        is_marketing,
        NTILE(4) OVER (ORDER BY is_marketing, id) AS is_marketing_bucket,
        SUM(budget_rounded) OVER (PARTITION BY location ORDER BY budget_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_rounded
    FROM base
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        location,
        budget_rounded,
        dept_name_filled,
        is_marketing,
        running_budget_rounded,
        is_marketing_bucket - budget_rounded AS is_marketing_bucket_minus_budget_rounded
    FROM with_windows
    WHERE location IS NOT NULL
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_job_title,
        top1.hire_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN is_marketing_bucket_minus_budget_rounded >= 84 THEN 'HIGH' ELSE 'LOW' END AS is_marketing_bucket_minus_budget_rounded_tier,
    AVG(budget_rounded) FILTER (WHERE employee_job_title = 'Director') AS director_avg_budget_rounded,
    PERCENTILE_APPROX(is_marketing_bucket_minus_budget_rounded, 0.9) AS is_marketing_bucket_minus_budget_rounded_p90,
    PERCENTILE_APPROX(running_budget_rounded, 0.9) AS running_budget_rounded_p90
FROM with_latest
GROUP BY is_marketing_bucket_minus_budget_rounded_tier;
