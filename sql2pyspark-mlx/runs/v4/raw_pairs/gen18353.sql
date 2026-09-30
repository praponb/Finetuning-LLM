WITH cleaned AS (
    SELECT
        *,
        ROUND(salary * 0.9, 2) salary_adjusted,
        TRIM(level) clean_level,
        COALESCE(level, 'DIRECT') level_filled
    FROM employees
    WHERE hire_date >= DATE '2024-01-31'
      AND level IS NOT NULL
),
metrics AS (
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
        level,
        salary_adjusted,
        clean_level,
        level_filled,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) event_seq,
        SUM(salary_adjusted) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_salary_adjusted
    FROM cleaned
),
ranked AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary_adjusted DESC, id) = 1
),
kept_keys AS (
    SELECT dept_id
    FROM ranked
    EXCEPT DISTINCT
    SELECT dept_id
    FROM employees
    WHERE manager_id IS NULL
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.dept_id = k.dept_id
),
valid AS (
    SELECT
        *,
        LOWER(level) level_lower,
        CEIL(event_seq) event_seq_whole,
        COALESCE(job_title, 'N/A') job_title_filled
    FROM reconciled
    WHERE hire_date > TO_DATE('2023-09-21')
      AND salary > 132000
)
SELECT *
FROM (
    SELECT
        level,
        salary,
        salary_adjusted
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (salary, salary_adjusted)
);
