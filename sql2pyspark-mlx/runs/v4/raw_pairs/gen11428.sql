WITH cleaned AS (
    -- Keep recent, valid rows
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
        UPPER(level) AS level_upper,
        COALESCE(job_title, 'NONE') AS resolved_job_title
    FROM employees
    WHERE hire_date > DATE '2023-06-29'
      AND job_title IS NOT NULL
      AND full_name IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS seq_no,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
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
        level_upper,
        resolved_job_title,
        row_seq,
        seq_no,
        last_known_job_title,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY hire_date, id) AS row_seq_2,
        LEAD(row_seq, 1) OVER (PARTITION BY level ORDER BY seq_no, id) AS next_row_seq
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY last_known_row_seq DESC, id) AS event_seq,
    SUM(next_row_seq) OVER (PARTITION BY job_title, level ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_row_seq
FROM enriched;
