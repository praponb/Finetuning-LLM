WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(level) AS level_lower
    FROM employees
    WHERE manager_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (ORDER BY salary, id) AS salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS event_seq,
        LAG(salary, 2) OVER (ORDER BY latest_rank, id) AS prev_salary,
        NTILE(3) OVER (PARTITION BY dept_id, job_title ORDER BY salary, id) AS salary_bucket_2
    FROM latest
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        level_lower,
        latest_rank,
        salary_bucket,
        salary_bucket_2,
        NTILE(4) OVER (PARTITION BY manager_id, dept_id ORDER BY salary_bucket_2, id) AS salary_bucket_2_bucket,
        ROW_NUMBER() OVER (ORDER BY latest_rank DESC, id) AS seq_no,
        AVG(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3,
        ROUND(salary_bucket / SUM(salary_bucket) OVER (PARTITION BY job_title, level), 4) AS salary_bucket_share,
        SUM(prev_salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_salary
    FROM with_windows
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.email,
        s.dept_id,
        s.manager_id,
        s.hire_date,
        s.seq_no,
        s.salary_avg_3,
        s.salary_bucket_share,
        s.running_prev_salary,
        d.budget
    FROM enriched s
    INNER JOIN departments d
        ON s.dept_id = d.id
    WHERE d.location IN ('Chiang Mai', 'Singapore')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        manager_id,
        hire_date,
        seq_no,
        salary_bucket_share,
        budget,
        DATE_TRUNC('year', hire_date) AS hire_date_period
    FROM enriched_employees
    WHERE running_prev_salary >= 655719
)
-- Grouped metrics
SELECT
    CASE WHEN salary_bucket_share >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_bucket_share_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(budget, 0.5) AS budget_p50,
    MAX(hire_date_period) AS last_hire_date_period,
    MIN(hire_date_period) AS first_hire_date_period,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles
FROM filtered
GROUP BY salary_bucket_share_tier
