WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        ADD_MONTHS(hire_date, 6) AS hire_date_plus_months
    FROM employees
    WHERE hire_date > TO_DATE('2023-08-03')
      AND job_title IS NOT NULL
),
with_dims AS (
    -- Attach departments attributes
    SELECT src.*, b.budget
    FROM filtered src
    LEFT JOIN departments b
        ON src.dept_id = b.id
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.*,
        e.job_title AS employee_job_title,
        e.salary,
        e.hire_date AS employee_hire_date
    FROM with_dims src
    INNER JOIN employees e
        ON src.manager_id <=> e.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        is_remote_label,
        budget,
        salary,
        SUM(salary) OVER (PARTITION BY manager_id ORDER BY hire_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        LEAD(budget, 1, 0) OVER (PARTITION BY manager_id ORDER BY hire_date_plus_months, id) AS next_budget,
        AVG(budget) OVER (PARTITION BY employee_job_title ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_avg_3,
        ROW_NUMBER() OVER (ORDER BY hire_date_plus_months DESC, id) AS seq_no
    FROM joined
)
-- Multi-level aggregation
SELECT
    job_title,
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    AVG(salary) FILTER (WHERE job_title = 'Director') AS director_avg_salary,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids,
    COUNT(*) FILTER (WHERE job_title = 'Analyst') AS analyst_rows,
    COUNT_IF(job_title = 'Analyst') AS analyst_events
FROM metrics
GROUP BY CUBE (job_title, DATE_TRUNC('YEAR', hire_date))
ORDER BY distinct_manager_ids NULLS FIRST
