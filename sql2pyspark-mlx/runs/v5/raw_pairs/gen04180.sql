WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        COALESCE(level, 'unassigned') AS level_filled,
        CONCAT(full_name, ' - ', level) AS full_name_level,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE level IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        level_filled,
        job_title_clean,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC) AS seq_no,
        NTILE(4) OVER w_part AS salary_bucket,
        RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM cleaned
    WHERE dept_id IN (12, 40)
    WINDOW w_part AS (PARTITION BY dept_id ORDER BY salary ASC, id ASC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        manager_id,
        level,
        level_filled,
        job_title_clean,
        salary_bucket,
        FIRST_VALUE(job_title, TRUE) OVER w_part AS first_known_job_title,
        ROW_NUMBER() OVER (ORDER BY salary_bucket DESC, id ASC) AS seq_no_2,
        SUM(salary_bucket) OVER (ORDER BY salary_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary_bucket DESC, id ASC) AS event_seq,
        NTILE(5) OVER (PARTITION BY manager_id ORDER BY salary DESC, id ASC) AS salary_bucket_2
    FROM metrics
    WINDOW w_part AS (ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        manager_id,
        level,
        level_filled,
        salary_bucket,
        seq_no_2,
        cumulative_salary_bucket,
        event_seq,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY manager_id), 4) AS event_seq_share,
        LAST_VALUE(salary_bucket_2, TRUE) OVER w_ordered AS last_known_salary_bucket_2,
        SUM(salary_bucket) OVER (PARTITION BY manager_id ORDER BY seq_no_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_bucket
    FROM scored
    WINDOW w_ordered AS (ORDER BY cumulative_salary_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        level,
        level_filled,
        salary_bucket,
        seq_no_2,
        cumulative_salary_bucket,
        event_seq_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY seq_no_2 ASC NULLS LAST, id ASC) AS dedup_rank
        FROM metrics_employees
    ) x
    WHERE dedup_rank = 1
),
cleaned_employees AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(level, 1, 3) AS level_prefix,
        COALESCE(level, 'UNKNOWN') AS level_clean
    FROM latest
    WHERE seq_no_2 < 6
)
SELECT
    id,
    level,
    level_filled,
    salary_bucket,
    cumulative_salary_bucket,
    level_prefix,
    level_clean,
    CASE WHEN level RLIKE '^(L2|L1)$' THEN 'PRIORITY' ELSE 'CORE' END AS level_zone
FROM cleaned_employees
ORDER BY level DESC
