WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        CONCAT(level, ', ', email) AS level_email,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM employees
    WHERE hire_date >= DATE '2023-05-18'
      AND level IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        hire_date,
        is_remote,
        job_title_clean,
        LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC) AS prev_salary,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY salary ASC, id ASC) AS event_seq
    FROM base
    WHERE MONTH(hire_date) IN (6, 9, 10)
),
enriched_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        hire_date,
        is_remote,
        job_title_clean,
        LEAD(event_seq, 1) OVER (ORDER BY prev_salary ASC, id ASC) AS next_event_seq,
        LEAD(prev_salary, 1) OVER w_recent AS next_prev_salary,
        SUM(event_seq) OVER w_part AS running_event_seq,
        SUM(event_seq) OVER w_part AS running_event_seq_2,
        NTILE(10) OVER (ORDER BY event_seq ASC, id ASC) AS event_seq_bucket
    FROM enriched
    WINDOW w_recent AS (ORDER BY hire_date ASC, id ASC), w_part AS (ORDER BY prev_salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(hire_date) AS hire_date_month_end,
        FLOOR(running_event_seq_2) AS running_event_seq_2_whole
    FROM enriched_employees
    WHERE hire_date >= DATE '2023-03-29'
      AND hire_date <= '2023-04-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        hire_date,
        job_title_clean,
        running_event_seq,
        running_event_seq_2,
        event_seq_bucket,
        running_event_seq_2_whole,
        MAX(running_event_seq) OVER w_grp AS last7_max_running_event_seq,
        AVG(running_event_seq) OVER w_grp AS last7_avg_running_event_seq,
        COUNT(running_event_seq) OVER w_grp AS last7_count_running_event_seq
    FROM valid
    WINDOW w_grp AS (ORDER BY next_prev_salary ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(last7_avg_running_event_seq) OVER (ORDER BY event_seq_bucket ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last7_avg_running_event_seq_avg_2,
        CUME_DIST() OVER (ORDER BY event_seq_bucket ASC NULLS LAST) AS event_seq_bucket_cume_dist,
        NTILE(5) OVER (ORDER BY running_event_seq DESC, id ASC) AS running_event_seq_bucket,
        RANK() OVER (ORDER BY running_event_seq_2 ASC NULLS LAST) AS running_event_seq_2_rank,
        NTILE(10) OVER (ORDER BY last7_count_running_event_seq DESC, id ASC) AS last7_count_running_event_seq_bucket
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    id,
    hire_date,
    job_title_clean,
    running_event_seq,
    event_seq_bucket,
    running_event_seq_2_whole,
    last7_count_running_event_seq,
    last7_avg_running_event_seq_avg_2,
    NTILE(5) OVER (ORDER BY event_seq_bucket DESC, id ASC) AS event_seq_bucket_bucket,
    LAST_VALUE(last7_avg_running_event_seq) IGNORE NULLS OVER (ORDER BY last7_avg_running_event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_avg_running_event_seq,
    ROW_NUMBER() OVER (ORDER BY running_event_seq DESC, id ASC) AS event_seq,
    ROW_NUMBER() OVER (ORDER BY running_event_seq_bucket ASC, id ASC) AS row_seq
FROM scored;
