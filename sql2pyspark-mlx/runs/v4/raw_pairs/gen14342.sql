WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(dept_name, ' ', location) AS dept_name_location,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE dept_name = 'Engineering'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.level,
        latest.employee_salary
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.level, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, location
    FROM with_last
    EXCEPT
    SELECT id, location
    FROM departments
    WHERE dept_name NOT IN ('Marketing', 'Finance')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN kept_keys k ON s.id = k.id AND s.location = k.location
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY level), 4) AS budget_share,
        CUME_DIST() OVER (PARTITION BY location, level ORDER BY employee_salary NULLS FIRST) AS employee_salary_cume_dist
    FROM kept_rows
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date >= s.employee_hire_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date >= s.employee_hire_date
        )
)
SELECT
    id,
    dept_name,
    location,
    dept_name_location,
    dept_name_title,
    location_clean,
    employee_hire_date,
    level,
    budget_share,
    CASE WHEN level RLIKE '^(L4|L3)$' THEN 'CORE' WHEN level RLIKE '^(L1|L2)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS level_zone
FROM eligible
ORDER BY budget_share;
