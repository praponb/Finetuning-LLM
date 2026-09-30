WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        TRIM(level) AS clean_level
    FROM employees
    WHERE hire_date >= DATE '2024-01-25'
      AND job_title IS NOT NULL
      AND full_name LIKE 'Ali%'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY job_title, manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title_2
    FROM valid
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.*,
        e.email AS employee_email,
        e.full_name AS employee_full_name,
        e.level
    FROM scored s
    LEFT JOIN employees e
        ON s.manager_id = e.id
)
SELECT
    id,
    dept_id,
    job_title,
    hire_date,
    is_remote,
    last_known_job_title_2,
    employee_email,
    employee_full_name,
    level,
    CASE WHEN level RLIKE '^(L1|L2)$' THEN 'GROUP_B' WHEN level RLIKE '^(L3|L4)$' THEN 'EXTENDED' ELSE 'CORE' END AS level_zone
FROM joined
ORDER BY job_title DESC NULLS FIRST;
