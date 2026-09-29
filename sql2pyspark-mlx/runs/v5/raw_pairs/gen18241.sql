WITH with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.level,
        top1.employee_job_title
    FROM departments s
    JOIN LATERAL (
            SELECT c.level, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary < 128000
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        level,
        LEAD(budget) OVER w_part AS next_budget,
        LAG(budget) OVER w_part AS prev_budget,
        ROW_NUMBER() OVER w_part AS row_seq,
        NTILE(10) OVER w_recent AS budget_bucket
    FROM not_flagged
    WHERE level IN ('L1', 'L5')
    WINDOW w_part AS (PARTITION BY employee_job_title ORDER BY budget ASC, id ASC), w_recent AS (PARTITION BY location ORDER BY budget ASC, id ASC)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.level,
        s.prev_budget,
        s.budget_bucket,
        x.employee_job_title
    FROM scored s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(prev_budget, 1, 0) OVER (PARTITION BY level ORDER BY budget_bucket ASC, id ASC) AS next_prev_budget,
        SUM(budget_bucket) OVER (ORDER BY prev_budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_bucket,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY level ORDER BY prev_budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        ROUND(prev_budget / SUM(prev_budget) OVER (PARTITION BY dept_name), 4) AS prev_budget_share,
        LEAD(prev_budget, 2, 0) OVER (PARTITION BY dept_name ORDER BY prev_budget ASC, id ASC) AS next_prev_budget_2
    FROM latest_match
)
-- Grouped metrics
SELECT
    CASE WHEN prev_budget_share >= 0.88 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_share_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(level = 'L3') AS l3_events,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM metrics
GROUP BY prev_budget_share_tier
ORDER BY l3_events DESC NULLS FIRST;
