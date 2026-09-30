WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        LENGTH(job_title) AS job_title_length,
        LOWER(job_title) AS job_title_lower,
        COALESCE(level, 'N/A') AS level_clean
    FROM employees
    WHERE hire_date > DATE '2024-01-27'
      AND level = 'L4'
)
-- Ranking and gap features
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    hire_date,
    is_remote,
    level,
    job_title_length,
    job_title_lower,
    level_clean,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS event_seq,
    NTILE(5) OVER (PARTITION BY manager_id ORDER BY job_title_length, id) AS job_title_length_bucket,
    NTILE(3) OVER (ORDER BY job_title_length, id) AS job_title_length_bucket_2,
    AVG(job_title_length) OVER w_recent AS job_title_length_avg_6_prior,
    SUM(job_title_length) OVER w_part AS running_job_title_length
FROM filtered
WINDOW w_recent AS (ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_part AS (PARTITION BY level ORDER BY job_title_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
