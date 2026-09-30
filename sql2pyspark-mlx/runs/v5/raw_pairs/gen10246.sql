WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        level,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(job_title, 'NONE') AS job_title_filled
    FROM employees
    WHERE hire_date > TO_DATE('2023-02-06')
      AND MONTH(hire_date) IN (3, 7, 10)
),
enriched AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.email,
        e.manager_id,
        e.is_remote_flag,
        e.job_title_filled,
        p2.salary,
        p2.job_title AS employee_job_title
    FROM base e
    INNER JOIN employees p2
        ON e.manager_id = p2.id
    WHERE p2.level = 'L3'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_remote_flag) OVER (PARTITION BY manager_id ORDER BY salary ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS is_remote_flag_avg_6,
        NTILE(5) OVER (PARTITION BY employee_job_title ORDER BY is_remote_flag ASC, id ASC) AS is_remote_flag_bucket
    FROM enriched
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, employee_job_title ORDER BY is_remote_flag_avg_6 DESC NULLS FIRST, id ASC) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    email,
    manager_id,
    is_remote_flag,
    salary,
    employee_job_title,
    is_remote_flag_bucket,
    rank_in_group
FROM ranked
ORDER BY id ASC;
