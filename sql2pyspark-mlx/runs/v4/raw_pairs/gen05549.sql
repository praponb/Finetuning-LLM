WITH filtered AS (
    SELECT
        *,
        CASE WHEN budget >= 4326000 THEN 'High' WHEN budget >= 811000 THEN 'Medium' ELSE 'Low' END AS budget_band,
        COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_dense_rank,
        RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC) AS budget_rank,
        NTILE(4) OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS budget_bucket,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM filtered
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email IS NOT NULL
        )
),
cleaned AS (
    SELECT
        id,
        location,
        budget,
        budget_band,
        location_filled,
        budget_dense_rank,
        budget_rank,
        budget_bucket,
        budget_share,
        CEIL(budget_share) AS budget_share_whole,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled
    FROM kept
    WHERE dept_name IS NOT NULL
      AND budget >= 3343000
)
SELECT
    id,
    location,
    budget_band,
    budget_rank,
    budget_bucket,
    budget_share,
    budget_share_whole,
    dept_name_filled,
    CASE WHEN location RLIKE '^(New York|Singapore)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS location_zone
FROM cleaned;
