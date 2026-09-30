WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        UPPER(TRIM(full_name)) AS full_name_normalized
    FROM employees
    WHERE job_title IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        DAY(hire_date) AS hire_date_day
    FROM valid
    WHERE hire_date > DATE '2023-05-16'
      AND job_title IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        hire_date_day,
        COALESCE(job_title, 'N/A') AS job_title_filled,
        COALESCE(job_title, 'NONE') AS job_title_filled_2
    FROM cleaned
    WHERE hire_date > TO_DATE('2023-03-22')
      AND job_title IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN hire_date_day >= 67 THEN 'HIGH' ELSE 'LOW' END AS hire_date_day_tier,
    ROUND(SUM(hire_date_day) / NULLIF(COUNT(CASE WHEN level = 'L2' THEN 1 END), 0), 2) AS hire_date_day_per_l2,
    PERCENTILE_APPROX(hire_date_day, ARRAY(0.25, 0.5, 0.75)) AS hire_date_day_quartiles,
    SUM(CASE WHEN level = 'L1' THEN hire_date_day ELSE 0 END) AS l1_hire_date_day
FROM base
GROUP BY ROLLUP (CASE WHEN hire_date_day >= 67 THEN 'HIGH' ELSE 'LOW' END)
HAVING ROUND(SUM(hire_date_day) / NULLIF(COUNT(CASE WHEN level = 'L2' THEN 1 END), 0), 2) >= 0
ORDER BY hire_date_day_tier NULLS LAST;
