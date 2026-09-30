WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        email,
        dept_id,
        job_title,
        hire_date,
        is_remote
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS row_num
        FROM employees
    ) x
    WHERE row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(job_title, TRUE) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title
    FROM latest
    WHERE hire_date <= '2024-10-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        job_title,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS seq_no_2
    FROM scored
),
scored_employees AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        seq_no,
        row_seq,
        seq_no_2,
        MIN(seq_no) OVER w_ordered AS running_min_seq_no,
        MAX(seq_no) OVER w_ordered AS running_max_seq_no,
        SUM(seq_no) OVER w_ordered AS running_sum_seq_no,
        COUNT(seq_no) OVER w_ordered AS running_count_seq_no
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY row_seq, id) AS row_num
        FROM scored_employees
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    job_title,
    seq_no,
    seq_no_2,
    running_min_seq_no,
    running_max_seq_no,
    row_num
FROM deduped;
