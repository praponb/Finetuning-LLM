WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', location, dept_name) location_dept_name_key
    FROM departments
    WHERE location IS NOT NULL
),
valid_departments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN dept_name IN ('Engineering', 'Marketing') THEN 'Tier 1' ELSE 'Other' END dept_name_group,
        COALESCE(location, 'OTHER') resolved_location
    FROM valid
    WHERE dept_name IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.employee_job_title
    FROM valid_departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, employee_job_title ORDER BY budget ASC, id ASC) AS rank_in_group
        FROM latest_match
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY employee_job_title, location), 4) budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) budget_share_2
    FROM ranked
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN budget_share_2 >= 0.71 THEN 'LARGE' ELSE 'SMALL' END AS budget_share_2_tier,
        employee_job_title,
        PERCENTILE_APPROX(budget_share, 0.9) AS budget_share_p90,
        SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget,
        PERCENTILE_APPROX(budget_share_2, 0.5) AS budget_share_2_p50,
        SORT_ARRAY(COLLECT_SET(employee_job_title)) AS employee_job_title_list,
        COUNT_IF(location = 'Tokyo') AS tokyo_events,
        RANK() OVER (PARTITION BY CASE WHEN budget_share_2 >= 0.71 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT_IF(location = 'Tokyo') DESC) AS tokyo_events_rank
    FROM scored
    GROUP BY CASE WHEN budget_share_2 >= 0.71 THEN 'LARGE' ELSE 'SMALL' END, employee_job_title
)
SELECT
    budget_share_2_tier,
    employee_job_title,
    budget_share_p90,
    new_york_budget,
    budget_share_2_p50
FROM daily
ORDER BY budget_share_2_tier ASC
