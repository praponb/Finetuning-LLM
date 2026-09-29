WITH with_windows AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY budget DESC, id ASC) AS budget_bucket,
        COUNT(budget) OVER (PARTITION BY location) AS location_count_budget,
        RANK() OVER (PARTITION BY dept_name, location ORDER BY budget ASC) AS budget_rank,
        NTILE(4) OVER (ORDER BY budget DESC, id ASC) AS budget_bucket_2
    FROM departments
    WHERE location NOT IN ('Berlin', 'Singapore')
),
metrics AS (
    SELECT
        *,
        LAG(budget_bucket) OVER (PARTITION BY location, dept_name ORDER BY location_count_budget ASC, id ASC) AS prev_budget_bucket,
        LEAD(budget_bucket) OVER (PARTITION BY location, dept_name ORDER BY location_count_budget ASC, id ASC) AS next_budget_bucket,
        LEAD(budget_rank) OVER (PARTITION BY location, dept_name ORDER BY location_count_budget ASC, id ASC) AS next_budget_rank,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY location_count_budget ASC, id ASC) AS seq_no,
        SUM(location_count_budget) OVER (PARTITION BY dept_name) AS dept_name_sum_location_count_budget
    FROM with_windows
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY prev_budget_bucket ASC NULLS FIRST, id ASC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
eligible AS (
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (6, 26, 38)
        )
),
scored AS (
    SELECT
        *,
        MIN(location_count_budget) OVER (PARTITION BY location) AS location_min_location_count_budget,
        SUM(next_budget_bucket) OVER (ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_budget_bucket,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY next_budget_bucket DESC NULLS FIRST) AS next_budget_bucket_percent_rank,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share
    FROM eligible
),
latest_departments AS (
    SELECT
        id,
        dept_name,
        location,
        budget_bucket,
        budget_rank,
        next_budget_bucket,
        next_budget_rank,
        rn,
        location_min_location_count_budget,
        running_next_budget_bucket,
        next_budget_bucket_percent_rank,
        budget_share
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget_bucket DESC, id ASC) = 1
),
with_latest AS (
    SELECT
        s.*,
        x.job_title,
        x.employee_salary
    FROM latest_departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    job_title,
    ROUND(AVG(running_next_budget_bucket), 2) AS avg_running_next_budget_bucket,
    COUNT(DISTINCT job_title) AS distinct_job_titles,
    COUNT(*) AS cnt
FROM with_latest
GROUP BY ROLLUP (job_title)
HAVING ROUND(AVG(running_next_budget_bucket), 2) < 1000000
