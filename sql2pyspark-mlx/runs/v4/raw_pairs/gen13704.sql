WITH with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.location,
        d.dept_name
    FROM employees a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(level) clean_level,
        LENGTH(email) email_length,
        COALESCE(job_title, 'UNKNOWN') job_title_clean
    FROM with_dims
    WHERE LENGTH(full_name) >= 14
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY email_length NULLS LAST, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY rank_in_group DESC, id) row_seq,
        LAG(email_length, 1) OVER (PARTITION BY job_title ORDER BY salary, id) prev_email_length
    FROM deduped
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
        salary,
        hire_date,
        is_remote,
        level,
        location,
        dept_name,
        clean_level,
        email_length,
        job_title_clean,
        rank_in_group,
        row_seq,
        prev_email_length,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY row_seq DESC, id) row_seq_2,
        MIN(prev_email_length) OVER (PARTITION BY dept_name, manager_id) dept_name_min_prev_email_length,
        LAG(row_seq, 1, 0) OVER (PARTITION BY job_title ORDER BY hire_date, id) prev_row_seq,
        MAX(prev_email_length) OVER (PARTITION BY manager_id) manager_id_max_prev_email_length,
        NTILE(4) OVER (PARTITION BY location, job_title ORDER BY prev_email_length DESC, id) prev_email_length_bucket
    FROM with_windows
)
SELECT
    id,
    full_name,
    email,
    dept_id,
    hire_date,
    is_remote,
    dept_name,
    clean_level,
    email_length
FROM enriched
ORDER BY dept_id DESC NULLS LAST;
