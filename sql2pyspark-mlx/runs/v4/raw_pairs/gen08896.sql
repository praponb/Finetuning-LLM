WITH with_windows AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS salary_bucket,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_cume_dist,
        SUM(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM employees
),
with_dims AS (
    SELECT
        src.*,
        d.location,
        d.dept_name,
        d.budget
    FROM with_windows src
    LEFT JOIN departments d
        ON src.dept_id = d.id
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(budget, TRUE) OVER w_ordered AS first_known_budget,
        RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS salary_rank,
        NTILE(4) OVER (PARTITION BY location ORDER BY running_salary DESC, id) AS running_salary_bucket,
        SUM(running_salary) OVER (PARTITION BY dept_name, job_title ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_salary,
        SUM(budget) OVER (ORDER BY salary_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY salary_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(first_known_budget, TRUE) OVER w_running AS last_known_first_known_budget,
        NTILE(3) OVER (PARTITION BY job_title ORDER BY running_budget, id) AS running_budget_bucket,
        LEAD(salary_rank, 2) OVER (PARTITION BY manager_id ORDER BY budget, id) AS next_salary_rank
    FROM metrics
    WINDOW w_running AS (PARTITION BY dept_id ORDER BY salary_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_employees AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        salary_bucket,
        salary_cume_dist,
        running_salary,
        location,
        dept_name,
        budget,
        first_known_budget,
        salary_rank,
        running_salary_bucket,
        cumulative_running_salary,
        running_budget,
        last_known_first_known_budget,
        running_budget_bucket,
        next_salary_rank,
        LEAD(running_salary, 1) OVER (PARTITION BY job_title ORDER BY cumulative_running_salary, id) AS next_running_salary,
        AVG(cumulative_running_salary) OVER (PARTITION BY manager_id, level ORDER BY running_budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cumulative_running_salary_avg_2,
        ROUND(running_budget_bucket / SUM(running_budget_bucket) OVER (PARTITION BY level, manager_id), 4) AS running_budget_bucket_share,
        SUM(last_known_first_known_budget) OVER (PARTITION BY level, location) AS level_sum_last_known_first_known_budget
    FROM enriched
),
latest AS (
    SELECT *
    FROM metrics_employees
    QUALIFY RANK() OVER (PARTITION BY location ORDER BY salary_bucket NULLS FIRST) = 1
),
enriched_employees AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY next_salary_rank DESC, id) AS next_salary_rank_bucket,
        LAG(cumulative_running_salary_avg_2, 1) OVER (PARTITION BY dept_name, level ORDER BY first_known_budget, id) AS prev_cumulative_running_salary_avg_2,
        SUM(salary_rank) OVER (PARTITION BY manager_id, location ORDER BY cumulative_running_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_rank
    FROM latest
),
daily AS (
    SELECT
        CASE WHEN running_budget_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS running_budget_bucket_tier,
        DATE_TRUNC('WEEK', hire_date) AS week_start,
        dept_id,
        COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count,
        COUNT(DISTINCT job_title) AS distinct_job_titles,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN running_budget_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) DESC) AS finance_count_percent_rank
    FROM enriched_employees
    GROUP BY running_budget_bucket_tier, week_start, dept_id
)
SELECT
    running_budget_bucket_tier,
    week_start,
    dept_id,
    distinct_job_titles
FROM daily
ORDER BY running_budget_bucket_tier DESC
