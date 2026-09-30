WITH matched_keys AS (
    -- Set difference on keys
    SELECT job_title
    FROM employees
    EXCEPT DISTINCT
    SELECT job_title
    FROM employees
    WHERE hire_date < '2025-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.job_title = k.job_title
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(salary, 2) AS salary_rounded,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM reconciled
    WHERE job_title IS DISTINCT FROM 'Intern'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(salary_rounded * 1.1, 2) AS salary_rounded_scaled
    FROM filtered
    WHERE hire_date > DATE '2023-04-02'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_rank,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY salary_rounded NULLS LAST) AS salary_rounded_percent_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share,
        DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary_rounded NULLS LAST) AS salary_rounded_dense_rank
    FROM valid
    WHERE dept_id = 26
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        level,
        salary,
        salary_share,
        salary_rounded_scaled,
        salary_rounded_percent_rank
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (salary, salary_share, salary_rounded_scaled, salary_rounded_percent_rank)
);
