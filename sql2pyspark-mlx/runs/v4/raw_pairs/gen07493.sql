WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(budget * 100, 2) AS budget_scaled,
        COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND LOWER(TRIM(x.email)) = 'david.lee@gmail.com'
        )
)
-- Running and moving aggregates
SELECT
    id,
    dept_name,
    location,
    budget,
    budget_scaled,
    location_filled,
    latest_rank,
    CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget NULLS FIRST) AS budget_cume_dist,
    LEAD(budget_scaled, 1) OVER (PARTITION BY dept_name ORDER BY latest_rank, id) AS next_budget_scaled,
    AVG(budget_scaled) OVER (ORDER BY latest_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_scaled_avg_5_prior
FROM not_flagged
