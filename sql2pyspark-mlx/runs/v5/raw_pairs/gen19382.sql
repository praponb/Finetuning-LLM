WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(salary) OVER w_ordered AS next_salary,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share
    FROM employees
    WHERE level IN ('L5', 'L2', 'L4')
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY salary, id)
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.manager_id,
        a.job_title,
        a.salary,
        a.hire_date,
        a.level,
        a.next_salary,
        a.salary_share,
        e.job_title AS employee_job_title
    FROM with_windows a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY manager_id ORDER BY salary_share, id) AS salary_share_bucket,
        ROW_NUMBER() OVER (ORDER BY salary_share, id) AS seq_no
    FROM joined
),
joined_employees AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.manager_id,
        a.job_title,
        a.salary,
        a.level,
        a.next_salary,
        a.salary_share,
        a.seq_no,
        d.full_name,
        d.level AS employee_level
    FROM enriched a
    JOIN employees d
        ON a.manager_id = d.id
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT
        e.*,
        d.full_name AS employee_full_name,
        d.email,
        d.job_title AS employee_job_title
    FROM joined_employees e
    LEFT JOIN employees d
        ON e.manager_id <=> d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        manager_id,
        job_title,
        level,
        next_salary,
        salary_share,
        full_name,
        employee_level,
        employee_job_title,
        NTILE(3) OVER (ORDER BY next_salary DESC, id) AS next_salary_bucket,
        ROW_NUMBER() OVER (ORDER BY salary, id) AS event_seq
    FROM enriched_employees
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS salary_share_tier,
    ROUND(AVG(next_salary_bucket), 2) AS avg_next_salary_bucket,
    PERCENTILE_APPROX(next_salary, 0.5) AS next_salary_p50,
    COUNT_IF(employee_job_title = 'Analyst') AS analyst_events,
    COUNT_IF(employee_job_title = 'Analyst') AS analyst_events_2,
    SUM(next_salary_bucket) FILTER (WHERE employee_job_title = 'Director') AS director_total_next_salary_bucket
FROM metrics
GROUP BY CASE WHEN salary_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY salary_share_tier NULLS LAST
