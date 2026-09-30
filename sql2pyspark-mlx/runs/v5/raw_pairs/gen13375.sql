WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        QUARTER(hire_date) AS hire_date_quarter,
        REGEXP_REPLACE(email, ' ', '') AS email_slug
    FROM employees
    WHERE hire_date >= DATE '2023-04-18'
      AND manager_id IN (7, 33)
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.manager_id,
        a.job_title,
        a.salary,
        a.hire_date_quarter,
        a.email_slug,
        b.location
    FROM filtered a
    LEFT JOIN departments b
        ON a.dept_id = b.id
    WHERE b.location IN ('Tokyo', 'Bangkok')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        dept_id,
        hire_date_quarter,
        email_slug,
        location,
        NTILE(3) OVER (PARTITION BY location ORDER BY salary, id) AS salary_bucket,
        SUM(salary) OVER w_part AS cumulative_salary
    FROM with_dims
    WINDOW w_part AS (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM employees
    WHERE is_remote
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(salary_bucket, 0) AS salary_bucket_filled,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(location, 'OTHER') AS location_clean
    FROM verified
    WHERE location IS NOT NULL
)
-- Multi-level aggregation
SELECT
    location,
    ROUND(AVG(hire_date_quarter), 2) AS avg_hire_date_quarter,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count,
    COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM base
GROUP BY 1
HAVING COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) > 1
