WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('YEAR', hire_date) AS hire_date_period,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM employees
    WHERE hire_date >= TO_DATE('2023-03-18')
),
valid_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        hire_date,
        hire_date_period,
        level_filled,
        LOWER(job_title) AS job_title_lower
    FROM valid
    WHERE hire_date >= DATE '2023-07-30'
      AND manager_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid_employees
    INTERSECT ALL
    SELECT id
    FROM employees
    WHERE level <> 'L2'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_employees s
    JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share,
        COUNT(salary) OVER (PARTITION BY dept_id) AS dept_id_count_salary
    FROM reconciled
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 136000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    dept_id,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(dept_id_count_salary, ARRAY(0.25, 0.5, 0.75)) AS dept_id_count_salary_quartiles,
    PERCENTILE_APPROX(dept_id_count_salary, 0.5) AS dept_id_count_salary_p50
FROM enriched
GROUP BY CASE WHEN salary >= 136000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', hire_date), dept_id
