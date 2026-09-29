WITH valid AS (
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
latest_match AS (
    SELECT s.*, latest.employee_level
    FROM ranked s
    JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
daily AS (
    SELECT
        dept_name,
        CASE WHEN budget >= 3061000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
        location,
        COUNT(*) AS num_rows,
        ROUND(SUM(budget_int) / NULLIF(COUNT(CASE WHEN dept_name = 'HR' THEN 1 END), 0), 2) AS budget_int_per_hr,
        APPROX_PERCENTILE(budget_int, ARRAY(0.25, 0.5, 0.75)) AS budget_int_quartiles
    FROM latest_match
    GROUP BY dept_name, CASE WHEN budget >= 3061000 THEN 'LARGE' ELSE 'SMALL' END, location
)
SELECT
    dept_name,
    budget_tier,
    location,
    budget_int_per_hr,
    budget_int_quartiles,
    CASE WHEN dept_name RLIKE '^(Engineering|HR)$' THEN 'GROUP_A' WHEN dept_name RLIKE '^(Sales|Marketing)$' THEN 'CORE' ELSE 'PRIORITY' END AS dept_name_zone
FROM daily
