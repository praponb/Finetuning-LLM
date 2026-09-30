WITH filtered AS (
    -- Basic cleansing and defaults
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
        CAST(salary AS INT) AS salary_int,
        LOWER(job_title) AS job_title_lower,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM employees
    WHERE salary <= 103000
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(salary) OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS next_salary,
        LAG(salary) OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS prev_salary,
        LEAD(salary_int) OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS next_salary_int,
        LAG(salary_int) OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS prev_salary_int,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary ASC) AS salary_dense_rank
    FROM filtered
    WHERE is_remote
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY manager_id ORDER BY next_salary DESC, id ASC) AS next_salary_bucket,
        LEAD(next_salary_int, 1, 0) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC) AS next_next_salary_int,
        ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY next_salary_int DESC, id ASC) AS seq_no,
        NTILE(5) OVER (ORDER BY row_seq ASC, id ASC) AS row_seq_bucket
    FROM scored
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        salary_int,
        job_title_lower,
        resolved_level,
        next_salary,
        prev_salary,
        next_salary_int,
        row_seq,
        salary_dense_rank,
        next_salary_bucket,
        seq_no,
        row_seq_bucket,
        ROUND(seq_no * 1.15, 2) AS seq_no_scaled,
        CASE WHEN next_next_salary_int >= 76 THEN 'High' WHEN next_next_salary_int >= 29 THEN 'Medium' ELSE 'Low' END AS next_next_salary_int_band
    FROM enriched
    WHERE hire_date >= TO_DATE('2023-05-07')
      AND level IS DISTINCT FROM 'L3'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        job_title,
        salary,
        job_title_lower,
        next_salary,
        row_seq,
        salary_dense_rank,
        next_salary_bucket,
        row_seq_bucket,
        seq_no_scaled,
        MAX(next_salary) OVER w_grp AS running_max_next_salary,
        MIN(next_salary) OVER w_grp AS running_min_next_salary,
        SUM(next_salary) OVER w_grp AS running_sum_next_salary,
        LEAD(seq_no, 2, 0) OVER (PARTITION BY level ORDER BY salary ASC, id ASC) AS next_seq_no
    FROM base
    WINDOW w_grp AS (PARTITION BY job_title, level ORDER BY next_salary_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY row_seq ASC, id ASC) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        job_title_lower,
        next_salary,
        row_seq,
        salary_dense_rank,
        seq_no_scaled,
        running_max_next_salary,
        running_min_next_salary,
        dedup_rank,
        NTILE(10) OVER (PARTITION BY job_title ORDER BY seq_no_scaled DESC, id ASC) AS seq_no_scaled_bucket,
        LAG(row_seq_bucket, 2, 0) OVER (PARTITION BY job_title ORDER BY row_seq ASC, id ASC) AS prev_row_seq_bucket
    FROM ranked
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_min_next_salary >= 137000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_min_next_salary_tier,
    CASE WHEN seq_no_scaled >= 68 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_scaled_tier,
    PERCENTILE_APPROX(row_seq, 0.5) AS row_seq_p50,
    ROUND(AVG(running_min_next_salary), 2) AS avg_running_min_next_salary,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(running_max_next_salary), 2) AS avg_running_max_next_salary,
    GROUPING(CASE WHEN running_min_next_salary >= 137000 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM with_windows
GROUP BY CUBE (CASE WHEN running_min_next_salary >= 137000 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN seq_no_scaled >= 68 THEN 'LARGE' ELSE 'SMALL' END)
HAVING distinct_ids > 1
ORDER BY distinct_ids DESC;
