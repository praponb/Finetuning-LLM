WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        TRIM(email) AS clean_email
    FROM employees
    WHERE hire_date >= DATE '2023-04-12'
      AND level IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN job_title IN ('Intern', 'Engineer') THEN 'Priority' ELSE 'Other' END AS job_title_group
    FROM base
    WHERE LENGTH(full_name) < 5
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        job_title_group,
        LAG(salary) OVER w_hist AS prev_salary,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM filtered
    WHERE is_remote
    WINDOW w_hist AS (PARTITION BY job_title ORDER BY hire_date, id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY event_seq DESC NULLS FIRST, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        job_title,
        CASE WHEN prev_salary >= 159000 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_salary_tier,
        ROUND(AVG(prev_salary), 2) AS avg_prev_salary,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
        COUNT(DISTINCT job_title) AS distinct_job_titles,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY COUNT(DISTINCT job_title) DESC) AS distinct_job_titles_dense_rank
    FROM ranked
    GROUP BY job_title, CASE WHEN prev_salary >= 159000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    job_title,
    prev_salary_tier,
    avg_prev_salary,
    job_title_list,
    distinct_job_titles,
    CASE WHEN job_title RLIKE '^(Director|Analyst)$' THEN 'GROUP_B' WHEN job_title RLIKE '^(Engineer|Manager)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS job_title_zone
FROM daily
ORDER BY job_title DESC;
