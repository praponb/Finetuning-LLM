WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CONCAT_WS('-', email, level) AS email_level_key
    FROM employees
    WHERE manager_id IS NOT NULL
      AND level = 'L2'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        job_title,
        salary,
        is_remote,
        email_level_key,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS salary_dense_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY salary DESC, id) AS row_seq,
        RANK() OVER (PARTITION BY level ORDER BY salary) AS salary_rank
    FROM cleaned
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_rank DESC, id) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
deduped_employees AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_share DESC NULLS FIRST, id) AS row_num_2
    FROM deduped
    QUALIFY row_num_2 = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped_employees
    QUALIFY RANK() OVER (PARTITION BY job_title ORDER BY salary_share DESC NULLS FIRST) = 1
),
deduped_employees_2 AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) = 1
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        job_title,
        CASE WHEN salary_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS salary_rank_tier,
        STDDEV_SAMP(salary) AS salary_stddev,
        AVG(salary_share) FILTER (WHERE job_title = 'Intern') AS intern_avg_salary_share,
        ROUND(SUM(salary_share) / NULLIF(COUNT(CASE WHEN job_title = 'Designer' THEN 1 END), 0), 2) AS salary_share_per_designer,
        COUNT(*) AS cnt,
        GROUPING_ID() AS gid
    FROM deduped_employees_2
    GROUP BY GROUPING SETS ((job_title, CASE WHEN salary_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END), (job_title), ())
)
SELECT
    job_title,
    salary_stddev,
    intern_avg_salary_share,
    cnt,
    gid,
    CASE WHEN job_title RLIKE '^(Intern|Director)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS job_title_zone
FROM grouped
ORDER BY intern_avg_salary_share DESC NULLS LAST
