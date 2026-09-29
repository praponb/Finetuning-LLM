WITH base AS (
    SELECT
        *,
        CASE WHEN budget >= 4006000 THEN 'High' WHEN budget >= 2053000 THEN 'Medium' ELSE 'Low' END AS budget_band,
        COALESCE(location, 'DIRECT') AS location_filled
    FROM departments
    WHERE location IN ('Tokyo', 'London', 'Chiang Mai')
),
filtered AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_band,
        location_filled,
        ROUND(budget * 1.15, 2) AS budget_scaled
    FROM base
    WHERE location = 'London'
),
matched_keys AS (
    SELECT dept_name, id
    FROM filtered
    INTERSECT ALL
    SELECT dept_name, id
    FROM departments
    WHERE location = 'Chiang Mai'
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
metrics AS (
    SELECT
        dept_name,
        location,
        budget,
        location_filled,
        MAX(budget) OVER (PARTITION BY location) AS location_max_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share
    FROM verified
    WHERE dept_name <> 'Legal'
),
enriched AS (
    SELECT
        budget,
        location_max_budget,
        budget_share,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget NULLS LAST) AS budget_percent_rank,
        SUM(budget_share) OVER (PARTITION BY location) AS location_sum_budget_share
    FROM metrics
)
SELECT
    CASE WHEN budget_percent_rank >= 0.72 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_percent_rank_tier,
    STDDEV_SAMP(budget) AS budget_stddev,
    PERCENTILE_APPROX(budget_percent_rank, 0.95) AS budget_percent_rank_p95,
    ROUND(AVG(location_sum_budget_share), 2) AS avg_location_sum_budget_share
FROM enriched
GROUP BY CASE WHEN budget_percent_rank >= 0.72 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(location_sum_budget_share), 2) < 1000000
