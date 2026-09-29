WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IN ('Tokyo', 'Chiang Mai')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_lower,
        SUBSTRING(location, 1, 4) AS location_prefix,
        LOWER(location) AS location_lower,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean_2
    FROM cleaned
    WHERE location NOT IN ('Tokyo', 'London', 'Bangkok')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget,
        s.location_prefix,
        s.location_lower,
        s.dept_name_clean_2,
        s.row_num,
        latest.employee_job_title,
        latest.employee_level
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        dept_name_clean_2,
        employee_job_title,
        PERCENT_RANK() OVER (PARTITION BY employee_level ORDER BY budget DESC NULLS LAST) AS budget_percent_rank,
        ROUND(budget / SUM(budget) OVER (PARTITION BY employee_job_title), 4) AS budget_share,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_dense_rank,
        AVG(budget) OVER (PARTITION BY employee_job_title, employee_level ORDER BY row_num, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5,
        ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY row_num DESC, id) AS row_seq
    FROM with_latest
    WHERE location = 'London'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_dense_rank DESC NULLS LAST, id) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    employee_job_title,
    COUNT(*) AS cnt,
    MAX(budget_avg_5) FILTER (WHERE location = 'London') AS london_max_budget_avg_5
FROM deduped
GROUP BY employee_job_title
HAVING COUNT(*) < 1000
