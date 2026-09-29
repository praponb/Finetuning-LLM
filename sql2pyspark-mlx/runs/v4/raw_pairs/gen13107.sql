WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(hire_date) AS hire_date_month_end,
        LOWER(email) AS email_lower,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM employees
    WHERE manager_id IS NOT NULL
      AND full_name LIKE '%For%'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date NULLS LAST, id) = 1
),
latest_employees AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date_month_end DESC NULLS FIRST, id) = 1
),
latest_employees_2 AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date_month_end DESC, id) AS dedup_rank
        FROM latest_employees
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT a.*, p.location
    FROM latest_employees_2 a
    INNER JOIN departments p
        ON a.dept_id = p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        manager_id,
        job_title,
        salary
    FROM enriched
) src
PIVOT (
    SUM(salary) AS total,
    COUNT(1) AS item_count
    FOR job_title IN ('Designer', 'Manager')
);
