WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM departments
    WHERE location NOT IN ('Bangkok', 'New York')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id IS NOT NULL
        )
),
filtered_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM kept
    WHERE dept_name IS NOT NULL
      AND budget_filled < 63
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.is_remote
        )
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.salary,
        latest.employee_job_title
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
daily AS (
    -- Multi-level aggregation
    SELECT
        location,
        SUM(CASE WHEN location = 'Berlin' THEN budget_filled ELSE 0 END) AS berlin_budget_filled,
        SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
        COUNT(*) AS num_rows
    FROM with_last
    GROUP BY location
    HAVING COUNT(*) < 1000
)
SELECT
    location,
    dept_name_list,
    num_rows,
    CASE WHEN location RLIKE '^(Tokyo|Chiang Mai)$' THEN 'PRIORITY' WHEN location RLIKE '^(Bangkok|New York)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS location_zone
FROM daily
ORDER BY location_zone NULLS LAST
