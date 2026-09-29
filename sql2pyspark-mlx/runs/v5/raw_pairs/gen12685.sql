WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        CONCAT(level, '/', full_name) AS level_full_name,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        COALESCE(level, 'UNKNOWN') AS resolved_level
    FROM employees
    WHERE hire_date > TO_DATE('2023-03-08')
      AND level IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY salary ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5,
        NTILE(10) OVER (PARTITION BY manager_id ORDER BY salary ASC, id ASC) AS salary_bucket,
        LEAD(salary, 1) OVER (ORDER BY hire_date ASC, id ASC) AS next_salary
    FROM filtered
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        IF(is_remote, 'Yes', 'No') AS is_remote_label_2,
        COALESCE(job_title, 'DIRECT') AS job_title_filled
    FROM with_windows
    WHERE hire_date >= DATE '2023-03-05'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        level_full_name,
        is_remote_label
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
with_windows_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY dept_id, job_title ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_salary,
        MIN(salary) OVER (PARTITION BY dept_id, job_title ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary,
        COUNT(salary) OVER (PARTITION BY dept_id, job_title ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_salary,
        MAX(salary) OVER (PARTITION BY dept_id, job_title ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
        MAX(salary) OVER (PARTITION BY dept_id, job_title) AS dept_id_max_salary
    FROM latest
)
-- Join a dimension and aggregate
SELECT
    d.location,
    a.job_title,
    COUNT(*) AS cnt,
    MAX(a.running_avg_salary) AS max_running_avg_salary,
    ROUND(AVG(a.running_min_salary), 2) AS avg_running_min_salary,
    GROUPING_ID() AS gid
FROM with_windows_employees a
LEFT JOIN departments d ON a.dept_id = d.id
GROUP BY GROUPING SETS ((d.location, a.job_title), (d.location), ())
ORDER BY avg_running_min_salary DESC NULLS FIRST
