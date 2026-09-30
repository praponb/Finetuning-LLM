WITH filtered AS (
    SELECT
        *,
        CONCAT(full_name, '/', job_title) full_name_job_title,
        CASE WHEN job_title IN ('Engineer', 'Designer') THEN 'Group A' ELSE 'Other' END job_title_group,
        COALESCE(job_title, 'NONE') resolved_job_title
    FROM employees
    WHERE manager_id IS NOT NULL
      AND manager_id IN (1, 36)
),
with_windows AS (
    SELECT
        id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        full_name_job_title,
        AVG(salary) OVER w_part last3_avg_salary,
        COUNT(salary) OVER w_part last3_count_salary,
        MAX(salary) OVER w_part last3_max_salary,
        NTILE(3) OVER w_part_2 salary_bucket
    FROM filtered
    WHERE job_title = 'Engineer'
    WINDOW w_part AS (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY salary, id)
),
cleaned AS (
    SELECT
        *,
        LENGTH(job_title) job_title_length,
        CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END is_engineer,
        COALESCE(job_title, 'N/A') job_title_clean
    FROM with_windows
    WHERE hire_date > TO_DATE('2023-05-12')
      AND job_title IS NOT DISTINCT FROM 'Manager'
),
enriched AS (
    SELECT
        *,
        MIN(is_engineer) OVER (PARTITION BY manager_id) manager_id_min_is_engineer,
        NTILE(3) OVER (PARTITION BY level ORDER BY last3_max_salary, id) last3_max_salary_bucket
    FROM cleaned
),
with_dims AS (
    SELECT
        src.*,
        d.email,
        d.hire_date AS employee_hire_date
    FROM enriched src
    LEFT JOIN employees d
        ON src.manager_id <=> d.id
)
SELECT
    DATE_TRUNC('MONTH', employee_hire_date) AS activity_month,
    level,
    ROUND(AVG(last3_max_salary_bucket), 2) AS avg_last3_max_salary_bucket,
    PERCENTILE_APPROX(last3_avg_salary, 0.95) AS last3_avg_salary_p95,
    COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', employee_hire_date), level), (DATE_TRUNC('MONTH', employee_hire_date)))
