WITH matched_keys AS (
    -- Set difference on keys
    SELECT job_title
    FROM employees
    EXCEPT
    SELECT job_title
    FROM employees
    WHERE job_title NOT IN ('Analyst', 'Director', 'Designer')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    JOIN matched_keys k ON s.job_title = k.job_title
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share_2,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share_3,
        COUNT(salary) OVER (PARTITION BY dept_id) AS dept_id_count_salary,
        AVG(salary) OVER w_recent AS level_avg_salary
    FROM kept_rows
    WHERE level = 'L4'
    WINDOW w_recent AS (PARTITION BY level)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(email)) AS email_normalized,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM enriched
    WHERE hire_date >= DATE '2023-04-11'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT job_title
    FROM valid
    EXCEPT
    SELECT job_title
    FROM employees
    WHERE dept_id IN (16, 29, 37, 38)
),
kept_rows_employees AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.job_title = k.job_title
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        dept_id,
        salary,
        salary_share
    FROM kept_rows_employees
) src
UNPIVOT (
    value FOR metric IN (salary, salary_share)
) unp;
