WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        ROW_NUMBER() OVER (ORDER BY hire_date DESC, id ASC) AS row_seq
    FROM employees
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC) = 1
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        b.email AS employee_email,
        b.job_title AS employee_job_title
    FROM latest s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
metrics AS (
    -- Running and moving aggregates
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
        salary_share,
        row_seq,
        employee_email,
        employee_job_title,
        SUM(salary) OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        AVG(salary_share) OVER (PARTITION BY dept_id ORDER BY row_seq ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_share_avg_5_prior
    FROM with_dims
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_salary >= 602146 THEN 'HIGH' ELSE 'LOW' END AS cumulative_salary_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    employee_job_title,
    MAX(salary_share) AS max_salary_share,
    COUNT_IF(job_title = 'Manager') AS manager_events,
    SUM(CASE WHEN employee_job_title = 'Intern' THEN salary_share_avg_5_prior ELSE 0 END) AS intern_salary_share_avg_5_prior,
    SUM(CASE WHEN employee_job_title = 'Engineer' THEN salary_share_avg_5_prior ELSE 0 END) AS engineer_salary_share_avg_5_prior,
    ROUND(AVG(row_seq), 2) AS avg_row_seq
FROM metrics
GROUP BY CASE WHEN cumulative_salary >= 602146 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', hire_date), employee_job_title
HAVING SUM(CASE WHEN employee_job_title = 'Intern' THEN salary_share_avg_5_prior ELSE 0 END) > 0
ORDER BY avg_row_seq DESC NULLS LAST;
