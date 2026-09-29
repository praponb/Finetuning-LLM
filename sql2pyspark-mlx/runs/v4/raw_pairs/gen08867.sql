WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(budget) AS budget_whole,
        COALESCE(dept_name, 'DIRECT') AS resolved_dept_name
    FROM departments
    WHERE location IN ('Berlin', 'Chiang Mai', 'Bangkok')
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.salary
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_whole,
        resolved_dept_name,
        salary,
        NTILE(5) OVER (PARTITION BY location ORDER BY salary DESC, id) AS salary_bucket,
        ROW_NUMBER() OVER (ORDER BY budget_whole DESC, id) AS row_seq
    FROM with_latest
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email LIKE '%@outlook.com'
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT dept_name, location
    FROM kept
    INTERSECT ALL
    SELECT dept_name, location
    FROM departments
    WHERE budget < 3305000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name AND s.location = k.location
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN budget >= 3368000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
        CASE WHEN budget_whole >= 75 THEN 'HIGH' ELSE 'LOW' END AS budget_whole_tier,
        ROUND(SUM(salary_bucket) / NULLIF(COUNT(CASE WHEN dept_name = 'HR' THEN 1 END), 0), 2) AS salary_bucket_per_hr,
        MAX(salary_bucket) FILTER (WHERE location = 'Tokyo') AS tokyo_max_salary_bucket,
        SIZE(COLLECT_SET(dept_name)) AS num_dept_names
    FROM kept_rows
    GROUP BY budget_tier, budget_whole_tier
)
SELECT
    budget_tier,
    budget_whole_tier,
    salary_bucket_per_hr,
    tokyo_max_salary_bucket,
    num_dept_names
FROM stats;
