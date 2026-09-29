WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(job_title, 1, 2) job_title_prefix,
        COALESCE(level, 'UNKNOWN') level_filled
    FROM employees
    WHERE salary > 149000
      AND manager_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, dept_id ORDER BY hire_date, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(salary) OVER w_part running_max_salary,
        AVG(salary) OVER w_part running_avg_salary,
        SUM(salary) OVER w_part running_sum_salary,
        COUNT(salary) OVER w_part running_count_salary,
        NTILE(5) OVER w_running salary_bucket
    FROM latest
    WINDOW w_part AS (PARTITION BY manager_id, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY level ORDER BY salary, id)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY rank_in_group DESC, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY running_avg_salary DESC, id) = 1
),
cleaned_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        level_filled,
        rank_in_group,
        running_max_salary,
        running_avg_salary,
        CAST(running_max_salary AS INT) running_max_salary_int,
        UPPER(full_name) full_name_upper
    FROM deduped
    WHERE is_remote = FALSE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(salary) OVER w_running job_title_min_salary,
        DENSE_RANK() OVER w_grp salary_dense_rank
    FROM cleaned_employees
    WINDOW w_running AS (PARTITION BY job_title), w_grp AS (PARTITION BY level, job_title ORDER BY salary DESC)
)
-- Grouped metrics
SELECT
    CASE WHEN running_avg_salary >= 156000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_avg_salary_tier,
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    PERCENTILE_APPROX(salary, 0.95) AS salary_p95
FROM with_windows
GROUP BY CASE WHEN running_avg_salary >= 156000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', hire_date)
ORDER BY salary_p95 DESC;
