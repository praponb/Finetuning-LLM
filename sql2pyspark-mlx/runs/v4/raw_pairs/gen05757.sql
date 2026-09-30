WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        INITCAP(TRIM(job_title)) job_title_title,
        CAST(is_remote AS INT) is_remote_flag,
        COALESCE(job_title, 'NONE') job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-09-18'
      AND level IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY manager_id, dept_id ORDER BY salary ASC, id ASC) salary_bucket,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_job_title,
        SUM(is_remote_flag) OVER (PARTITION BY level, manager_id ORDER BY is_remote_flag ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_remote_flag
    FROM cleaned
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.dept_name,
        d.budget,
        d.location
    FROM with_windows src
    JOIN departments d
        ON src.dept_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS rn
        FROM with_dims
    ) ranked
    WHERE rn = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        level,
        job_title_title,
        job_title_clean,
        salary_bucket,
        running_is_remote_flag,
        dept_name,
        budget,
        location,
        rn,
        CONCAT(full_name, ', ', level) full_name_level,
        LOWER(email) email_lower
    FROM deduped
    WHERE level IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    job_title,
    AVG(salary_bucket) FILTER (WHERE dept_name = 'Marketing') AS marketing_avg_salary_bucket,
    COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count,
    APPROX_PERCENTILE(salary_bucket, ARRAY(0.25, 0.5, 0.75)) AS salary_bucket_quartiles
FROM filtered
GROUP BY job_title;
