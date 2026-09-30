WITH base AS (
    SELECT *, FLOOR(budget) AS budget_whole
    FROM departments
    WHERE dept_name IN ('HR', 'Engineering')
),
filtered AS (
    SELECT
        *,
        TRIM(location) AS clean_location,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM base
    WHERE budget_whole >= 49
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary <= 106000
        )
),
kept_keys AS (
    SELECT location
    FROM kept
    INTERSECT
    SELECT location
    FROM departments
    WHERE budget <= 3634000
),
kept_rows AS (
    SELECT s.*
    FROM kept s
    JOIN kept_keys k ON s.location = k.location
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER w_hist AS budget_whole_cume_dist,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share_2
    FROM kept_rows
    WHERE dept_name = 'Marketing'
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget_whole DESC)
),
base_departments AS (
    SELECT
        *,
        LOWER(location) AS location_lower,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM enriched
    WHERE dept_name <> 'Engineering'
),
with_windows AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_whole,
        clean_location,
        location_lower,
        resolved_location,
        RANK() OVER (ORDER BY budget_share NULLS LAST) AS budget_share_rank
    FROM base_departments
)
SELECT
    CASE WHEN budget_whole >= 85 THEN 'HIGH' ELSE 'LOW' END AS budget_whole_tier,
    AVG(budget_whole) FILTER (WHERE location = 'New York') AS new_york_avg_budget_whole,
    MAX(budget_share_rank) FILTER (WHERE location = 'Bangkok') AS bangkok_max_budget_share_rank,
    COUNT(*) FILTER (WHERE location = 'Bangkok') AS bangkok_rows
FROM with_windows
GROUP BY 1
ORDER BY bangkok_rows DESC NULLS LAST
