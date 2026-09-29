WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(job_title, 'OTHER') AS job_title_clean
    FROM employees
    WHERE manager_id IS NOT NULL
      AND level IS DISTINCT FROM 'L3'
),
enriched AS (
    SELECT *, FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM cleaned
),
with_windows AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROW_NUMBER() OVER w_ordered AS seq_no_2,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY dept_id ORDER BY hire_date, id), w_part AS (PARTITION BY job_title ORDER BY hire_date, id)
),
deduped AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY seq_no DESC NULLS FIRST, id) = 1
),
enriched_employees AS (
    SELECT a.*, p.dept_name
    FROM deduped a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
enriched_employees_2 AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY job_title ORDER BY seq_no_2 DESC, id) AS seq_no_2_bucket,
        FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY job_title, dept_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
        NTILE(4) OVER (PARTITION BY dept_name ORDER BY seq_no_2, id) AS seq_no_2_bucket_2
    FROM enriched_employees
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY seq_no_2 DESC) AS seq_no_2_cume_dist,
        LAG(seq_no_2_bucket, 1) OVER w_ordered AS prev_seq_no_2_bucket,
        LAG(seq_no, 1) OVER w_ordered_2 AS prev_seq_no,
        LEAD(seq_no, 1, 0) OVER w_running AS next_seq_no
    FROM enriched_employees_2
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY hire_date, id), w_ordered_2 AS (ORDER BY seq_no, id), w_running AS (PARTITION BY dept_name, dept_id ORDER BY seq_no_2, id)
)
SELECT
    *,
    LEAD(seq_no_2) OVER (PARTITION BY dept_name ORDER BY seq_no_2, id) AS next_seq_no_2,
    LAG(seq_no_2) OVER (PARTITION BY dept_name ORDER BY seq_no_2, id) AS prev_seq_no_2,
    LAG(seq_no) OVER (PARTITION BY dept_name ORDER BY seq_no_2, id) AS prev_seq_no_3,
    ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY seq_no_2, id) AS row_seq_2
FROM scored
WHERE seq_no_2_bucket_2 BETWEEN 7 AND 8
