WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        job_title,
        level,
        FIRST_VALUE(level) IGNORE NULLS OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level, dept_id), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id, manager_id), 4) AS salary_share_2
    FROM employees
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        job_title,
        first_known_level,
        salary_share,
        salary_share_2,
        TRIM(job_title) AS clean_job_title,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM with_windows
    WHERE job_title IS NOT NULL
),
with_windows_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(salary_share) OVER (PARTITION BY job_title ORDER BY salary_share, id) AS next_salary_share,
        LAG(salary_share) OVER (PARTITION BY job_title ORDER BY salary_share, id) AS prev_salary_share,
        LAG(salary_share_2) OVER (PARTITION BY job_title ORDER BY salary_share, id) AS prev_salary_share_2,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_share, id) AS row_seq
    FROM filtered
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY prev_salary_share, id) AS row_num
        FROM with_windows_employees
    ) t
    WHERE row_num = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(row_num) AS row_num_whole,
        COALESCE(job_title, 'OTHER') AS resolved_job_title
    FROM latest
    WHERE job_title IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(prev_salary_share_2) OVER (PARTITION BY job_title ORDER BY prev_salary_share_2, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS prev_salary_share_2_avg_5,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY job_title), 4) AS row_seq_share,
        NTILE(5) OVER (PARTITION BY job_title ORDER BY prev_salary_share, id) AS prev_salary_share_bucket,
        AVG(prev_salary_share_2) OVER (PARTITION BY job_title ORDER BY row_num_whole, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_salary_share_2_avg_3_prior,
        LAG(prev_salary_share_2, 1, 0) OVER (PARTITION BY job_title ORDER BY prev_salary_share_2, id) AS prev_prev_salary_share_2
    FROM base
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_salary_share_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END AS prev_salary_share_bucket_tier,
        CASE WHEN prev_salary_share_2_avg_3_prior >= 1 THEN 'HIGH' ELSE 'LOW' END AS prev_salary_share_2_avg_3_prior_tier,
        job_title,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
        ROUND(AVG(row_seq), 2) AS avg_row_seq,
        AVG(prev_salary_share_2) AS avg_prev_salary_share_2
    FROM scored
    GROUP BY ROLLUP (CASE WHEN prev_salary_share_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_salary_share_2_avg_3_prior >= 1 THEN 'HIGH' ELSE 'LOW' END, job_title)
    HAVING avg_prev_salary_share_2 > 0
)
SELECT
    prev_salary_share_bucket_tier,
    prev_salary_share_2_avg_3_prior_tier,
    job_title,
    job_title_list,
    CASE WHEN job_title RLIKE '^(Intern|Designer)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS job_title_zone
FROM stats
ORDER BY prev_salary_share_2_avg_3_prior_tier NULLS LAST
