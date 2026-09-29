WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        ROUND(salary, 2) salary_rounded
    FROM employees
    WHERE hire_date > DATE '2023-06-11'
      AND job_title IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_rounded DESC NULLS LAST, id) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        dept_id,
        job_title,
        salary,
        dedup_rank,
        COUNT(salary) OVER (PARTITION BY level) level_count_salary,
        STDDEV_SAMP(salary) OVER (PARTITION BY job_title, dept_id) job_title_stddev_samp_salary
    FROM ranked
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(salary) OVER (PARTITION BY job_title ORDER BY salary, id) next_salary,
        LAG(job_title_stddev_samp_salary) OVER (PARTITION BY job_title ORDER BY salary, id) prev_job_title_stddev_samp_salary,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) row_seq
    FROM metrics
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_id,
            job_title,
            job_title_stddev_samp_salary
        FROM with_windows
    ) src
    PIVOT (
        MAX(job_title_stddev_samp_salary) AS amount
        FOR job_title IN ('Designer', 'Director')
    )
)
SELECT
    dept_id,
    Designer,
    Director
FROM by_type
ORDER BY dept_id DESC, Director;
