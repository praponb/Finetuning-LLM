WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level IN ('L5', 'L4') THEN 'Priority' ELSE 'Other' END AS level_group
    FROM employees
    WHERE hire_date > DATE '2023-03-23'
      AND is_remote = FALSE
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(hire_date, 1) AS hire_date_plus_months
    FROM cleaned
    WHERE hire_date >= TO_DATE('2023-06-15')
      AND job_title IS DISTINCT FROM 'Analyst'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        level_group,
        STDDEV_SAMP(salary) OVER (PARTITION BY job_title) AS job_title_stddev_samp_salary,
        NTILE(4) OVER w_running AS salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id, manager_id), 4) AS salary_share,
        COUNT(salary) OVER (PARTITION BY level, dept_id) AS level_count_salary
    FROM base
    WINDOW w_running AS (PARTITION BY level, job_title ORDER BY salary, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(salary_bucket) OVER (PARTITION BY manager_id) AS manager_id_min_salary_bucket,
        ROW_NUMBER() OVER w_hist AS row_seq,
        NTILE(10) OVER w_running AS job_title_stddev_samp_salary_bucket,
        NTILE(5) OVER (ORDER BY job_title_stddev_samp_salary, id) AS job_title_stddev_samp_salary_bucket_2
    FROM deduped
    WINDOW w_hist AS (PARTITION BY manager_id, level ORDER BY dedup_rank DESC, id), w_running AS (PARTITION BY manager_id ORDER BY job_title_stddev_samp_salary, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        manager_id,
        CASE WHEN level_count_salary >= 18 THEN 'HIGH' ELSE 'LOW' END AS level_count_salary_tier,
        SIZE(COLLECT_SET(level)) AS num_levels,
        SUM(CASE WHEN level = 'L3' THEN level_count_salary ELSE 0 END) AS l3_level_count_salary,
        SUM(CASE WHEN level = 'L4' THEN job_title_stddev_samp_salary_bucket_2 ELSE 0 END) AS l4_job_title_stddev_samp_salary_bucket_2,
        SIZE(COLLECT_SET(level)) AS num_levels_2,
        AVG(job_title_stddev_samp_salary_bucket_2) FILTER (WHERE level = 'L4') AS l4_avg_job_title_stddev_samp_salary_bucket_2,
        COUNT(DISTINCT id) AS distinct_ids
    FROM enriched
    GROUP BY manager_id, CASE WHEN level_count_salary >= 18 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    s.manager_id,
    s.level_count_salary_tier,
    s.num_levels,
    s.l4_job_title_stddev_samp_salary_bucket_2,
    s.num_levels_2,
    s.l4_avg_job_title_stddev_samp_salary_bucket_2,
    (SELECT SUM(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS sum_employee_salary
FROM totals s
ORDER BY num_levels_2 DESC;
