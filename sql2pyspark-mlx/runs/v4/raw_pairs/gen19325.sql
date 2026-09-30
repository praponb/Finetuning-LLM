WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM departments
    WHERE location = 'Tokyo'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_level
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(budget_int, TRUE) OVER (PARTITION BY employee_level ORDER BY budget_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget_int,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget_int DESC, id) AS budget_int_bucket,
        NTILE(3) OVER w_part AS budget_bucket
    FROM latest_match_departments
    WINDOW w_part AS (PARTITION BY employee_level ORDER BY budget, id)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.job_title
    FROM metrics s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    dept_name,
    location,
    budget_int,
    resolved_dept_name,
    employee_level,
    budget_bucket,
    job_title,
    CASE WHEN dept_name RLIKE '^(HR|Legal)$' THEN 'GROUP_B' WHEN dept_name RLIKE '^(Engineering|Sales)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS dept_name_zone
FROM with_last
ORDER BY budget_int DESC NULLS LAST;
