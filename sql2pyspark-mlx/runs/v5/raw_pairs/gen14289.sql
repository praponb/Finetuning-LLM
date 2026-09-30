WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(dept_name, 'unassigned') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        dept_name_filled,
        FIRST_VALUE(budget, TRUE) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share
    FROM filtered
    WHERE dept_name IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        first_known_budget,
        budget_share,
        DENSE_RANK() OVER w_recent AS first_known_budget_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY first_known_budget NULLS FIRST) AS first_known_budget_percent_rank,
        LAG(budget_share, 1) OVER (PARTITION BY dept_name ORDER BY first_known_budget, id) AS prev_budget_share,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY first_known_budget DESC NULLS FIRST) AS first_known_budget_cume_dist,
        AVG(budget_share) OVER (ORDER BY budget_share, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_share_avg_5_prior
    FROM with_windows
    WINDOW w_recent AS (ORDER BY first_known_budget NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date <= '2025-04-01'
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_budget_dense_rank >= 6 THEN 'HIGH' ELSE 'LOW' END AS first_known_budget_dense_rank_tier,
    CASE WHEN first_known_budget_cume_dist >= 0.6 THEN 'LARGE' ELSE 'SMALL' END AS first_known_budget_cume_dist_tier,
    PERCENTILE_APPROX(first_known_budget_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS first_known_budget_cume_dist_quartiles,
    ROUND(AVG(budget_share_avg_5_prior), 2) AS avg_budget_share_avg_5_prior,
    PERCENTILE_APPROX(first_known_budget_dense_rank, 0.5) AS first_known_budget_dense_rank_p50
FROM kept
GROUP BY CASE WHEN first_known_budget_dense_rank >= 6 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN first_known_budget_cume_dist >= 0.6 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY first_known_budget_cume_dist_tier;
