WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(location) AS location_lower,
        COALESCE(location, 'UNKNOWN') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name IN ('Engineering', 'Sales', 'Marketing')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share,
        LEAD(budget, 2) OVER (ORDER BY budget ASC, id ASC) AS next_budget,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location
    FROM valid
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND LENGTH(ch.full_name) > 8
        )
),
kept_departments AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND LENGTH(x.email) >= 9
        )
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(next_budget / SUM(next_budget) OVER (PARTITION BY location), 4) AS next_budget_share,
    AVG(budget) OVER (PARTITION BY dept_name) AS dept_name_avg_budget,
    ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id ASC) AS seq_no
FROM kept_departments
