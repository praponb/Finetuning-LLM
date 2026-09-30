WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(job_title) AS clean_job_title
    FROM employees
    WHERE level IS NOT NULL
      AND job_title IS DISTINCT FROM 'Intern'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        job_title,
        salary,
        is_remote,
        level,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS seq_no
    FROM base
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY job_title, level ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_seq_no,
        COUNT(seq_no) OVER (PARTITION BY job_title, level ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_seq_no,
        AVG(seq_no) OVER (PARTITION BY job_title, level ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_seq_no,
        SUM(event_seq) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq
    FROM metrics
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY job_title ORDER BY running_event_seq DESC NULLS FIRST) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(running_sum_seq_no) OVER (PARTITION BY job_title, level ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_sum_seq_no,
        MAX(running_sum_seq_no) OVER (PARTITION BY job_title, level ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_sum_seq_no,
        SUM(running_sum_seq_no) OVER (PARTITION BY job_title, level ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_sum_seq_no
    FROM ranked
    WHERE level <> 'L2'
),
kept_keys AS (
    -- Set difference on keys
    SELECT job_title
    FROM scored
    EXCEPT DISTINCT
    SELECT job_title
    FROM employees
    WHERE level IN ('L4', 'L3', 'L5')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.job_title = k.job_title
)
SELECT
    full_name,
    job_title,
    running_count_seq_no,
    running_avg_seq_no
FROM reconciled
ORDER BY running_count_seq_no DESC NULLS FIRST
