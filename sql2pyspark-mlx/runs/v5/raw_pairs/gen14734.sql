WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(dept_name, 'Unknown') AS dept_name_filled,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM departments
    WHERE dept_name IN ('Marketing', 'Sales', 'Legal')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS event_seq,
        LAST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location,
        LAG(budget, 1) OVER w_running AS prev_budget,
        SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget
    FROM ranked
    WHERE dept_name = 'HR'
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY rn, id)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.level
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        resolved_dept_name,
        rn,
        event_seq,
        last_known_location,
        level,
        NTILE(4) OVER (PARTITION BY location, level ORDER BY dept_name_sum_budget DESC, id) AS dept_name_sum_budget_bucket,
        LAST_VALUE(dept_name_sum_budget, TRUE) OVER (PARTITION BY location ORDER BY dept_name_sum_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name_sum_budget,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY event_seq, id) AS row_seq
    FROM latest_match
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.resolved_dept_name,
        s.rn,
        s.last_known_dept_name_sum_budget,
        top1.hire_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN last_known_dept_name_sum_budget >= 15692950 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_dept_name_sum_budget_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    MAX(last_known_dept_name_sum_budget) FILTER (WHERE location = 'Tokyo') AS tokyo_max_last_known_dept_name_sum_budget,
    SUM(last_known_dept_name_sum_budget) AS total_last_known_dept_name_sum_budget,
    COUNT(*) FILTER (WHERE location = 'Singapore') AS singapore_rows,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    MIN(last_known_dept_name_sum_budget) AS min_last_known_dept_name_sum_budget,
    GROUPING_ID() AS grouping_id
FROM latest_match_departments
GROUP BY GROUPING SETS ((CASE WHEN last_known_dept_name_sum_budget >= 15692950 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', hire_date)), (CASE WHEN last_known_dept_name_sum_budget >= 15692950 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING tokyo_max_last_known_dept_name_sum_budget > 0
ORDER BY last_known_dept_name_sum_budget_tier
