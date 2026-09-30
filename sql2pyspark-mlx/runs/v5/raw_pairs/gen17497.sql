WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(job_title, 1, 3) AS job_title_prefix
    FROM employees
    WHERE manager_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        full_name,
        job_title,
        hire_date,
        level
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        full_name,
        job_title,
        hire_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC NULLS LAST, id) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(job_title) AS job_title_lower,
        COALESCE(job_title, 'DIRECT') AS job_title_clean
    FROM ranked
    WHERE MONTH(hire_date) IN (6, 7, 11)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        hire_date,
        job_title_clean
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids
FROM deduped
GROUP BY DATE_TRUNC('MONTH', hire_date)
ORDER BY activity_month DESC NULLS LAST;
