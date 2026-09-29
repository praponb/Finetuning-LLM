WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        LOWER(level) AS level_lower
    FROM employees
    WHERE level IS NOT NULL
      AND job_title IS DISTINCT FROM 'Intern'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level ORDER BY dedup_rank) AS rank_in_group
        FROM ranked
    ) x
    WHERE rank_in_group = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        level_lower,
        dedup_rank,
        rank_in_group,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM deduped
    WHERE MONTH(hire_date) IN (1, 7, 8)
      AND job_title IS NOT NULL
)
-- Multi-level aggregation
SELECT
    level,
    MIN(salary) AS min_salary,
    MIN(salary) AS min_salary_2,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS grouping_id
FROM base
GROUP BY ROLLUP (level)
