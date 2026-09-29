WITH valid AS (
    SELECT
        *,
        COALESCE(job_title, 'Unknown') AS job_title_filled,
        FLOOR(salary) AS salary_whole
    FROM employees
    WHERE job_title IS NOT NULL
),
base AS (
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
        job_title_filled,
        salary_whole,
        CONCAT_WS('/', email, full_name) AS email_full_name_key,
        DATE_SUB(hire_date, 30) AS hire_date_minus_30d
    FROM valid
    WHERE salary_whole BETWEEN 64 AND 72
),
base_employees AS (
    SELECT
        *,
        COALESCE(level, 'unassigned') AS level_filled,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM base
    WHERE level IS NOT NULL
      AND level IN ('L3', 'L5', 'L2')
)
SELECT
    CASE WHEN salary_whole >= 64 THEN 'HIGH' ELSE 'LOW' END AS salary_whole_tier,
    DATE_TRUNC('MONTH', hire_date_minus_30d) AS activity_month,
    SIZE(COLLECT_SET(level)) AS num_levels,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    PERCENTILE_APPROX(salary_whole, ARRAY(0.25, 0.5, 0.75)) AS salary_whole_quartiles
FROM base_employees
GROUP BY GROUPING SETS ((CASE WHEN salary_whole >= 64 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', hire_date_minus_30d)), (CASE WHEN salary_whole >= 64 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY num_rows
