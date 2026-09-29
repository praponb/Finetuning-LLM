WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id) AS row_num
        FROM departments
    ) t
    WHERE row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(budget) OVER (PARTITION BY location) AS location_count_budget,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget NULLS LAST) AS budget_rank
    FROM latest
    WHERE dept_name IN ('Sales', 'HR')
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        top1.employee_hire_date,
        top1.job_title
    FROM metrics s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(budget) OVER w_part AS last3_sum_budget,
        MAX(budget) OVER w_part AS last3_max_budget,
        AVG(budget) OVER w_part AS last3_avg_budget,
        COUNT(budget) OVER w_part AS last3_count_budget,
        AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior
    FROM with_last
    WINDOW w_part AS (PARTITION BY job_title ORDER BY employee_hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN budget_avg_5_prior >= 4429439 THEN 'LARGE' ELSE 'SMALL' END AS budget_avg_5_prior_tier,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN location = 'Tokyo' THEN last3_max_budget ELSE 0 END) AS tokyo_last3_max_budget,
    APPROX_PERCENTILE(budget_avg_5_prior, 0.9) AS budget_avg_5_prior_p90,
    ROUND(SUM(budget_avg_5_prior) / NULLIF(COUNT(CASE WHEN job_title = 'Intern' THEN 1 END), 0), 2) AS budget_avg_5_prior_per_intern,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count
FROM enriched
GROUP BY CASE WHEN budget_avg_5_prior >= 4429439 THEN 'LARGE' ELSE 'SMALL' END
HAVING APPROX_PERCENTILE(budget_avg_5_prior, 0.9) > 0;
