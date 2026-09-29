WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        IF(is_remote, 'Yes', 'No') AS is_remote_label
    FROM employees
    WHERE hire_date >= DATE '2023-11-05'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY hire_date, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST, id) = 1
)
-- Running and moving aggregates
SELECT
    *,
    FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
    ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY latest_rank DESC, id) AS row_seq
FROM deduped;
