WITH cleaned AS (
    SELECT
        *,
        LOWER(level) AS level_lower,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE hire_date >= DATE '2023-04-06'
      AND salary >= 97000
),
filtered AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        level_lower,
        is_remote_flag,
        LOWER(level) AS level_lower_2,
        TRIM(full_name) AS clean_full_name
    FROM cleaned
    WHERE manager_id IS NOT NULL
),
valid AS (
    SELECT
        id,
        dept_id,
        job_title,
        hire_date,
        is_remote_flag,
        clean_full_name,
        DATE_TRUNC('MONTH', hire_date) AS hire_date_period,
        SUBSTRING(full_name, 1, 2) AS full_name_prefix,
        COALESCE(level, 'OTHER') AS level_filled
    FROM filtered
    WHERE dept_id IN (10, 14)
),
cleaned_employees AS (
    SELECT
        id,
        job_title,
        is_remote_flag,
        hire_date_period,
        level_filled,
        ROUND(is_remote_flag * 1.15, 2) AS is_remote_flag_adjusted,
        LENGTH(job_title) AS job_title_length,
        COALESCE(job_title, 'DIRECT') AS job_title_clean
    FROM valid
    WHERE job_title IS NOT NULL
      AND dept_id IN (11, 33)
),
scored AS (
    SELECT
        id,
        job_title,
        hire_date_period,
        is_remote_flag_adjusted,
        job_title_length,
        job_title_clean,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY is_remote_flag NULLS FIRST) AS is_remote_flag_cume_dist,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY is_remote_flag_adjusted DESC NULLS LAST) AS is_remote_flag_adjusted_cume_dist,
        LAST_VALUE(is_remote_flag) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_remote_flag,
        DENSE_RANK() OVER (ORDER BY job_title_length DESC) AS job_title_length_dense_rank,
        AVG(is_remote_flag) OVER (PARTITION BY job_title ORDER BY CAST(hire_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_remote_flag_avg_30d
    FROM cleaned_employees
    WHERE TO_DATE(hire_date_period) >= '2025-04-01'
)
SELECT
    job_title,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    (UNIX_TIMESTAMP(MAX(hire_date_period)) - UNIX_TIMESTAMP(MIN(hire_date_period))) / 60 AS span_minutes
FROM scored
GROUP BY 1;
