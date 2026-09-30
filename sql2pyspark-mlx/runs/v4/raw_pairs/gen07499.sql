WITH enriched AS (
    -- Ranking and gap features
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
        NTILE(5) OVER (ORDER BY salary, id) AS salary_bucket,
        SUM(salary) OVER w_running AS running_salary
    FROM employees
    WINDOW w_running AS (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(salary_bucket, TRUE) OVER w_hist AS first_known_salary_bucket,
        FIRST_VALUE(level, TRUE) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        RANK() OVER w_recent AS running_salary_rank
    FROM enriched
    WINDOW w_hist AS (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY dept_id, job_title ORDER BY running_salary DESC)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY running_salary_rank DESC, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY dept_id, level ORDER BY running_salary_rank DESC NULLS FIRST) AS running_salary_rank_cume_dist,
        STDDEV_SAMP(first_known_salary_bucket) OVER (PARTITION BY level, dept_id) AS level_stddev_samp_first_known_salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY hire_date, id) AS seq_no,
        LAST_VALUE(salary_bucket) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY running_salary_rank, id) AS row_seq
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    PERCENTILE_APPROX(running_salary_rank, 0.5) AS running_salary_rank_p50,
    COUNT(*) FILTER (WHERE level = 'L5') AS l5_rows,
    PERCENTILE_APPROX(running_salary, ARRAY(0.25, 0.5, 0.75)) AS running_salary_quartiles
FROM metrics
GROUP BY DATE_TRUNC('QUARTER', hire_date)
ORDER BY quarter_start DESC;
