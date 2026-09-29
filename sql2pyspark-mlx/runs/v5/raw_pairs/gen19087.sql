WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(email) AS email_lower,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM employees
    WHERE manager_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        email,
        job_title,
        salary,
        hire_date,
        level,
        email_lower,
        level_filled
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) = 1
),
latest_employees AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
latest_employees_2 AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY hire_date DESC, id) AS rank_in_group
    FROM latest_employees
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        job_title,
        salary
    FROM latest_employees_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY rank_in_group DESC, id) = 1
)
-- Grouped metrics
SELECT
    job_title,
    APPROX_PERCENTILE(salary, 0.5) AS salary_p50,
    MAX(salary) AS max_salary,
    GROUPING(job_title) AS gid
FROM ranked
GROUP BY ROLLUP (job_title)
ORDER BY salary_p50 DESC;
