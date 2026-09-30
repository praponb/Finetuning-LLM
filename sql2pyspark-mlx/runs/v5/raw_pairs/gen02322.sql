WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        TRIM(job_title) AS clean_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-04-17'
      AND is_remote = TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level = 'L4' THEN 1 ELSE 0 END AS is_l4
    FROM base
    WHERE MONTH(hire_date) IN (2, 10, 12)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id ASC) = 1
),
base_employees AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(salary) AS salary_whole
    FROM deduped
    WHERE is_l4 = 54
      AND manager_id IS NOT NULL
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.manager_id,
        src.job_title,
        src.clean_job_title,
        src.is_l4,
        src.salary_whole,
        d.job_title AS employee_job_title
    FROM base_employees src
    LEFT JOIN employees d
        ON src.manager_id <=> d.id
),
base_employees_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        manager_id,
        is_l4,
        salary_whole,
        employee_job_title,
        COALESCE(is_l4, 0) AS is_l4_filled
    FROM joined
    WHERE manager_id IS NULL
)
-- Join a dimension and aggregate
SELECT
    p.job_title,
    s.employee_job_title,
    COUNT(*) AS num_rows,
    ROUND(AVG(s.salary_whole), 2) AS avg_salary_whole,
    GROUPING_ID() AS level_id
FROM base_employees_2 s
INNER JOIN employees p ON s.manager_id = p.id
GROUP BY GROUPING SETS ((p.job_title, s.employee_job_title), (p.job_title), ())
ORDER BY num_rows ASC NULLS LAST;
