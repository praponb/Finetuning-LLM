WITH cleaned AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        LENGTH(level) AS level_length,
        SUBSTRING(level, 1, 2) AS level_prefix
    FROM employees
    WHERE hire_date >= DATE '2023-08-29'
      AND manager_id IS NOT NULL
),
metrics AS (
    SELECT
        *,
        MAX(level_length) OVER (PARTITION BY job_title ORDER BY level_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_level_length,
        SUM(level_length) OVER (PARTITION BY job_title ORDER BY level_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_level_length,
        AVG(level_length) OVER (PARTITION BY job_title ORDER BY level_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_level_length
    FROM cleaned
),
valid AS (
    SELECT
        *,
        INITCAP(TRIM(job_title)) AS job_title_title,
        DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label
    FROM metrics
    WHERE hire_date >= DATE '2023-04-14'
      AND manager_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY last4_max_level_length DESC NULLS LAST, id) = 1
),
joined AS (
    SELECT
        s.*,
        d.level,
        d.email,
        d.salary
    FROM latest s
    LEFT JOIN employees d
        ON s.manager_id = d.id
),
daily AS (
    SELECT
        dept_id,
        manager_id,
        SUM(CASE WHEN level = 'L2' THEN last4_max_level_length ELSE 0 END) AS l2_last4_max_level_length,
        COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count,
        PERCENTILE_APPROX(last4_avg_level_length, 0.5) AS last4_avg_level_length_p50,
        ROUND(AVG(last4_sum_level_length), 2) AS avg_last4_sum_level_length,
        DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) DESC) AS manager_count_dense_rank
    FROM joined
    GROUP BY dept_id, manager_id
)
SELECT
    s.dept_id,
    s.manager_id,
    s.manager_count,
    (SELECT MIN(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS min_employee_salary
FROM daily s
ORDER BY manager_count DESC NULLS FIRST
