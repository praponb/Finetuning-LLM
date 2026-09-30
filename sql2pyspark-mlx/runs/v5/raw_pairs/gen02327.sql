WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS next_budget,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS row_seq,
        AVG(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior
    FROM departments
    WHERE dept_name IS NOT NULL
),
scored_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        next_budget,
        row_seq,
        budget_avg_5_prior,
        MIN(budget_avg_5_prior) OVER (PARTITION BY location, dept_name ORDER BY next_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_budget_avg_5_prior,
        COUNT(budget_avg_5_prior) OVER (PARTITION BY location, dept_name ORDER BY next_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_budget_avg_5_prior,
        SUM(budget_avg_5_prior) OVER (PARTITION BY location, dept_name ORDER BY next_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_budget_avg_5_prior,
        MAX(budget_avg_5_prior) OVER (PARTITION BY location, dept_name ORDER BY next_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_budget_avg_5_prior
    FROM scored
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title = 'Intern'
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_job_title
    FROM eligible s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored_departments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY employee_job_title ORDER BY next_budget DESC NULLS LAST) AS next_budget_dense_rank,
        AVG(budget_avg_5_prior) OVER (PARTITION BY employee_job_title ORDER BY budget_avg_5_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_5_prior_avg_2,
        AVG(last7_count_budget_avg_5_prior) OVER (PARTITION BY employee_job_title ORDER BY last7_count_budget_avg_5_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_budget_avg_5_prior_avg_6
    FROM latest_match
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(budget_avg_5_prior_avg_2) OVER (PARTITION BY employee_job_title ORDER BY last7_sum_budget_avg_5_prior, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior_avg_2_avg_3_prior,
        ROUND(last7_min_budget_avg_5_prior / SUM(last7_min_budget_avg_5_prior) OVER (PARTITION BY employee_job_title), 4) AS last7_min_budget_avg_5_prior_share,
        ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY next_budget, id) AS row_seq_2
    FROM scored_departments_2
)
-- Multi-level aggregation
SELECT
    CASE WHEN last7_sum_budget_avg_5_prior >= 20348000 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_sum_budget_avg_5_prior_tier,
    CASE WHEN last7_min_budget_avg_5_prior >= 4361000 THEN 'LARGE' ELSE 'SMALL' END AS last7_min_budget_avg_5_prior_tier,
    COUNT(DISTINCT employee_job_title) AS distinct_employee_job_titles,
    COUNT(*) AS cnt,
    SUM(CASE WHEN employee_job_title = 'Intern' THEN next_budget ELSE 0 END) AS intern_next_budget,
    RANK() OVER (PARTITION BY CASE WHEN last7_sum_budget_avg_5_prior >= 20348000 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN employee_job_title = 'Intern' THEN next_budget ELSE 0 END) DESC) AS intern_next_budget_rank
FROM with_windows
GROUP BY CASE WHEN last7_sum_budget_avg_5_prior >= 20348000 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last7_min_budget_avg_5_prior >= 4361000 THEN 'LARGE' ELSE 'SMALL' END;
