WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN location IN ('Chiang Mai', 'Tokyo') THEN 'Group A' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id = 25
        )
),
eligible_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.dept_id IN (6, 18)
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_name
    FROM eligible_departments
    EXCEPT ALL
    SELECT dept_name
    FROM departments
    WHERE dept_name IN ('Legal', 'Engineering', 'HR')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible_departments s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        location,
        COUNT(*) FILTER (WHERE location = 'Berlin') AS berlin_rows,
        AVG(budget) FILTER (WHERE location = 'London') AS london_avg_budget,
        COUNT(*) AS num_rows,
        MAX(budget) FILTER (WHERE location = 'New York') AS new_york_max_budget,
        STDDEV_SAMP(budget) AS budget_stddev,
        ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS budget_per_engineering
    FROM kept_rows
    GROUP BY 1
)
SELECT
    location,
    berlin_rows,
    london_avg_budget,
    num_rows,
    new_york_max_budget,
    budget_stddev
FROM stats
ORDER BY new_york_max_budget DESC NULLS LAST
