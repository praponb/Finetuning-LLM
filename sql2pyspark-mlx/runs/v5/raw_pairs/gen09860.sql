WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT dept_name, id
    FROM departments
    INTERSECT ALL
    SELECT dept_name, id
    FROM departments
    WHERE location IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) budget_share,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST) budget_cume_dist
    FROM verified
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND NOT c.is_remote
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_recent budget_share_cume_dist,
        STDDEV_SAMP(budget_share) OVER (PARTITION BY location) location_stddev_samp_budget_share,
        MAX(budget_cume_dist) OVER w_part dept_name_max_budget_cume_dist,
        CUME_DIST() OVER (ORDER BY budget NULLS FIRST) budget_cume_dist_2,
        PERCENT_RANK() OVER w_grp budget_percent_rank
    FROM eligible
    WHERE budget_cume_dist <= 0.27
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget_share DESC NULLS FIRST), w_part AS (PARTITION BY dept_name), w_grp AS (ORDER BY budget NULLS LAST)
)
-- Grouped metrics
SELECT
    location,
    CASE WHEN budget_share >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_share_tier,
    MAX(dept_name_max_budget_cume_dist) AS max_dept_name_max_budget_cume_dist,
    APPROX_PERCENTILE(budget_share_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS budget_share_cume_dist_quartiles,
    COUNT(*) FILTER (WHERE location = 'Singapore') AS singapore_rows,
    RANK() OVER (PARTITION BY location ORDER BY MAX(dept_name_max_budget_cume_dist) DESC) AS max_dept_name_max_budget_cume_dist_rank
FROM metrics
GROUP BY location, CASE WHEN budget_share >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END;
