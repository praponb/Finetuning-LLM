WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        ROUND(salary * 1.07, 2) AS salary_adjusted,
        TRIM(job_title) AS clean_job_title
    FROM employees
    WHERE manager_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        clean_job_title,
        STDDEV_SAMP(salary_adjusted) OVER (PARTITION BY manager_id) AS manager_id_stddev_samp_salary_adjusted,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY level ORDER BY salary, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(manager_id_stddev_samp_salary_adjusted) OVER (PARTITION BY manager_id, job_title ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS manager_id_stddev_samp_salary_adjusted_avg_5_prior,
        LEAD(event_seq, 1) OVER (PARTITION BY job_title ORDER BY manager_id_stddev_samp_salary_adjusted, id) AS next_event_seq,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY event_seq DESC, id) AS event_seq_2
    FROM with_windows
),
enriched_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER w_hist AS salary_bucket,
        LAST_VALUE(event_seq_2) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq_2,
        SUM(manager_id_stddev_samp_salary_adjusted) OVER w_running AS running_manager_id_stddev_samp_salary_adjusted
    FROM enriched
    WHERE job_title NOT IN ('Intern', 'Manager', 'Director')
    WINDOW w_hist AS (PARTITION BY job_title ORDER BY salary, id), w_running AS (PARTITION BY manager_id ORDER BY manager_id_stddev_samp_salary_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        clean_job_title,
        event_seq_2,
        last_known_event_seq_2,
        NTILE(4) OVER (PARTITION BY manager_id ORDER BY last_known_event_seq_2, id) AS last_known_event_seq_2_bucket,
        SUM(salary) OVER (ORDER BY running_manager_id_stddev_samp_salary_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM enriched_employees
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY last_known_event_seq_2_bucket DESC NULLS LAST, id) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    job_title,
    last_known_event_seq_2,
    last_known_event_seq_2_bucket,
    CASE WHEN job_title RLIKE '^(Engineer|Analyst)$' THEN 'GROUP_B' WHEN job_title RLIKE '^(Manager|Director)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS job_title_zone
FROM ranked
ORDER BY job_title_zone DESC NULLS LAST;
