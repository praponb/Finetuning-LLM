WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        TRIM(level) AS clean_level,
        ROUND(salary, 2) AS salary_rounded
    FROM employees
    WHERE hire_date >= DATE '2023-05-07'
      AND level IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        dept_id,
        hire_date,
        is_remote,
        level,
        clean_level,
        COUNT(salary_rounded) OVER w_grp AS running_count_salary_rounded,
        MAX(salary_rounded) OVER w_grp AS running_max_salary_rounded,
        SUM(salary_rounded) OVER w_grp AS running_sum_salary_rounded,
        AVG(salary_rounded) OVER w_grp AS running_avg_salary_rounded,
        FIRST_VALUE(job_title, TRUE) OVER w_part AS first_known_job_title
    FROM base
    WINDOW w_grp AS (PARTITION BY manager_id, dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY job_title, manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM enriched
    EXCEPT
    SELECT level, id
    FROM employees
    WHERE dept_id IN (27, 38)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.level = k.level AND s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(running_sum_salary_rounded, 1) AS running_sum_salary_rounded_rounded,
        COALESCE(level, 'UNKNOWN') AS level_clean
    FROM kept_rows
    WHERE running_count_salary_rounded BETWEEN 6 AND 9
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(running_sum_salary_rounded * 1.07, 2) AS adjusted_running_sum_salary_rounded,
        COALESCE(level, 'OTHER') AS level_filled
    FROM filtered
    WHERE hire_date >= DATE '2023-05-30'
      AND level IS DISTINCT FROM 'L2'
)
SELECT
    dept_id,
    hire_date,
    clean_level,
    running_count_salary_rounded,
    running_max_salary_rounded,
    running_sum_salary_rounded,
    first_known_job_title,
    running_sum_salary_rounded_rounded,
    level_clean
FROM cleaned
ORDER BY running_sum_salary_rounded DESC NULLS FIRST;
