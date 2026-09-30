WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS row_seq,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM departments
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        row_seq,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget_share, id) AS budget_share_bucket,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket,
        LEAD(budget, 2) OVER (PARTITION BY dept_name, location ORDER BY row_seq, id) AS next_budget
    FROM enriched
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget_share_bucket,
        s.budget_bucket,
        latest.salary,
        latest.employee_job_title
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        budget_share_bucket,
        employee_job_title,
        COUNT(budget_share_bucket) OVER w_part AS last4_count_budget_share_bucket,
        MIN(budget_share_bucket) OVER w_part AS last4_min_budget_share_bucket,
        SUM(budget_share_bucket) OVER w_part AS last4_sum_budget_share_bucket
    FROM latest_match
    WINDOW w_part AS (PARTITION BY employee_job_title ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN last4_min_budget_share_bucket >= 9.1 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_min_budget_share_bucket_tier,
        employee_job_title,
        APPROX_PERCENTILE(last4_sum_budget_share_bucket, 0.95) AS last4_sum_budget_share_bucket_p95,
        SIZE(COLLECT_SET(employee_job_title)) AS num_employee_job_titles,
        APPROX_PERCENTILE(last4_min_budget_share_bucket, 0.5) AS last4_min_budget_share_bucket_p50
    FROM with_windows
    GROUP BY CASE WHEN last4_min_budget_share_bucket >= 9.1 THEN 'PREMIUM' ELSE 'STANDARD' END, employee_job_title
    HAVING APPROX_PERCENTILE(last4_min_budget_share_bucket, 0.5) >= 0
)
SELECT
    last4_min_budget_share_bucket_tier,
    employee_job_title,
    last4_sum_budget_share_bucket_p95
FROM totals
ORDER BY employee_job_title DESC NULLS LAST;
