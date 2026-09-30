WITH base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM departments
    WHERE dept_name = 'HR'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_3_prior,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location
    FROM base
),
base_departments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        budget - budget_share AS budget_minus_budget_share,
        LEAST(budget_share, budget) AS least_budget_share_budget
    FROM metrics
    WHERE budget <= 2405000
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.job_title
    FROM base_departments s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN least_budget_share_budget >= 90 THEN 'HIGH' ELSE 'LOW' END AS least_budget_share_budget_tier,
    COUNT(DISTINCT job_title) AS distinct_job_titles,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count
FROM with_latest
GROUP BY 1;
