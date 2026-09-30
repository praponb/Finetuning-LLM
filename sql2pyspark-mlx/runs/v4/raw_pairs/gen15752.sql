WITH filtered AS (
    SELECT
        *,
        LOWER(location) AS location_lower,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
),
metrics AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_lower,
        location_clean,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        CUME_DIST() OVER (PARTITION BY location ORDER BY budget NULLS LAST) AS budget_cume_dist,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2,
        AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5
    FROM filtered
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY location ORDER BY budget_share_2, id) AS budget_share_2_bucket,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget_cume_dist DESC NULLS LAST) AS budget_cume_dist_percent_rank,
        LEAD(budget_avg_5_prior, 2) OVER (PARTITION BY location ORDER BY budget_cume_dist, id) AS next_budget_avg_5_prior,
        LAST_VALUE(budget_avg_5_prior, TRUE) OVER w_hist AS last_known_budget_avg_5_prior
    FROM metrics
    WINDOW w_hist AS (ORDER BY budget_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN last_known_budget_avg_5_prior >= 3698612 THEN 'HIGH' ELSE 'LOW' END AS last_known_budget_avg_5_prior_tier,
    location,
    COUNT(CASE WHEN location = 'Singapore' THEN 1 END) AS singapore_count,
    SUM(CASE WHEN dept_name = 'Sales' THEN budget_avg_5 ELSE 0 END) AS sales_budget_avg_5,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN last_known_budget_avg_5_prior >= 3698612 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(CASE WHEN location = 'Singapore' THEN 1 END) DESC) AS singapore_count_percent_rank
FROM with_windows
GROUP BY CASE WHEN last_known_budget_avg_5_prior >= 3698612 THEN 'HIGH' ELSE 'LOW' END, location
ORDER BY location DESC NULLS FIRST
