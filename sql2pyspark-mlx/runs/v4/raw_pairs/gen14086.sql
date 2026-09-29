WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM departments
    WHERE budget > 1409000
),
filtered_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        budget,
        dept_name_title,
        CASE WHEN location IN ('New York', 'Tokyo') THEN 'Group A' ELSE 'Other' END AS location_group,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM filtered
    WHERE location <> 'Chiang Mai'
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_job_title
    FROM filtered_departments s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
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
        dept_name,
        budget,
        dept_name_title,
        location_group,
        resolved_dept_name,
        employee_job_title,
        AVG(budget) OVER (PARTITION BY employee_job_title ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
        SUM(budget) OVER (PARTITION BY employee_job_title ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget
    FROM with_latest
),
with_windows_departments AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        dept_name_title,
        location_group,
        resolved_dept_name,
        employee_job_title,
        budget_avg_2,
        cumulative_budget,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget NULLS FIRST) AS budget_rank,
        LAG(cumulative_budget, 1) OVER w_grp AS prev_cumulative_budget
    FROM with_windows
    WINDOW w_grp AS (ORDER BY budget, id)
)
SELECT
    id,
    budget,
    dept_name_title,
    location_group,
    employee_job_title,
    budget_avg_2,
    cumulative_budget,
    CASE WHEN employee_job_title RLIKE '^(Designer|Manager)$' THEN 'GROUP_A' WHEN employee_job_title RLIKE '^(Engineer|Analyst)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS employee_job_title_zone
FROM with_windows_departments
ORDER BY id DESC;
