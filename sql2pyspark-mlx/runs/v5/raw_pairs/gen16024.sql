WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(email) AS email_length,
        TRIM(email) AS clean_email
    FROM employees
    WHERE manager_id IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        is_remote,
        clean_email,
        salary * email_length AS salary_times_email_length,
        CONCAT_WS('/', job_title, email) AS job_title_email_key,
        COALESCE(level, 'OTHER') AS level_clean
    FROM valid
    WHERE hire_date >= TO_DATE('2023-05-31')
      AND level IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        manager_id,
        is_remote,
        job_title_email_key,
        SUM(salary_times_email_length) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_times_email_length,
        NTILE(4) OVER (PARTITION BY dept_id ORDER BY salary, id) AS salary_bucket,
        PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY salary_times_email_length DESC NULLS LAST) AS salary_times_email_length_percent_rank,
        STDDEV_SAMP(salary_times_email_length) OVER (PARTITION BY manager_id) AS manager_id_stddev_samp_salary_times_email_length
    FROM base
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY manager_id_stddev_samp_salary_times_email_length DESC) AS manager_id_stddev_samp_salary_times_email_length_dense_rank,
        FIRST_VALUE(salary_bucket, TRUE) OVER (PARTITION BY manager_id ORDER BY salary_times_email_length_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_bucket,
        MIN(salary_times_email_length_percent_rank) OVER (PARTITION BY manager_id) AS manager_id_min_salary_times_email_length_percent_rank
    FROM metrics
    WHERE UPPER(TRIM(email)) = 'BOB.SMITH@YAHOO.COM'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        is_remote,
        cumulative_salary_times_email_length,
        salary_times_email_length_percent_rank,
        first_known_salary_bucket,
        manager_id_min_salary_times_email_length_percent_rank,
        LAG(manager_id_stddev_samp_salary_times_email_length, 2) OVER w_grp AS prev_manager_id_stddev_samp_salary_times_email_length,
        COUNT(manager_id_stddev_samp_salary_times_email_length_dense_rank) OVER w_running AS manager_id_count_manager_id_stddev_samp_salary_times_email_length_dense_rank,
        AVG(first_known_salary_bucket) OVER (PARTITION BY manager_id ORDER BY first_known_salary_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_known_salary_bucket_avg_6
    FROM with_windows
    WINDOW w_grp AS (ORDER BY manager_id_min_salary_times_email_length_percent_rank, id), w_running AS (PARTITION BY manager_id)
)
SELECT
    id,
    email,
    is_remote,
    salary_times_email_length_percent_rank,
    first_known_salary_bucket,
    manager_id_min_salary_times_email_length_percent_rank,
    prev_manager_id_stddev_samp_salary_times_email_length,
    manager_id_count_manager_id_stddev_samp_salary_times_email_length_dense_rank
FROM enriched
ORDER BY prev_manager_id_stddev_samp_salary_times_email_length NULLS LAST;
