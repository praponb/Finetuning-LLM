WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(location, ' - ', dept_name) AS location_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.full_name IS NULL
        )
),
kept_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary < 65000
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.level,
        top1.hire_date
    FROM kept_departments s
    LEFT JOIN LATERAL (
            SELECT c.level, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    ROUND(AVG(budget), 2) AS avg_budget,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    APPROX_PERCENTILE(budget, 0.9) AS budget_p90,
    SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget,
    COUNT(DISTINCT location) AS distinct_locations,
    STDDEV_SAMP(budget) AS budget_stddev
FROM latest_match
GROUP BY DATE_TRUNC('YEAR', hire_date), DATE_TRUNC('QUARTER', hire_date)
HAVING budget_stddev > 0
ORDER BY avg_budget
