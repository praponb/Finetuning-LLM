WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM departments
    WHERE dept_name IN ('Sales', 'Finance', 'Marketing')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND YEAR(ch.hire_date) = 2025
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.salary
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (1, 26, 28)
        )
),
cleaned_departments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        COALESCE(location, 'DIRECT') AS resolved_location
    FROM not_flagged
    WHERE location <> 'New York'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary) OVER w_grp AS running_sum_salary,
        MIN(salary) OVER w_grp AS running_min_salary,
        COUNT(salary) OVER w_grp AS running_count_salary
    FROM cleaned_departments
    WINDOW w_grp AS (PARTITION BY location ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
daily AS (
    -- Multi-level aggregation
    SELECT
        dept_name,
        SORT_ARRAY(COLLECT_SET(location)) AS location_list,
        SUM(CASE WHEN location = 'Chiang Mai' THEN salary ELSE 0 END) AS chiang_mai_salary,
        COUNT(DISTINCT dept_name) AS distinct_dept_names,
        COUNT_IF(location = 'Tokyo') AS tokyo_events,
        COUNT(*) AS event_count
    FROM scored
    GROUP BY 1
)
SELECT
    dept_name,
    chiang_mai_salary,
    distinct_dept_names,
    tokyo_events,
    event_count
FROM daily
ORDER BY distinct_dept_names DESC NULLS FIRST, dept_name ASC;
