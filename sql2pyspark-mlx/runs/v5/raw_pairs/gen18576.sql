WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 0) AS budget_rounded,
        ROUND(budget * 1.07, 2) AS budget_adjusted
    FROM departments
    WHERE budget < 1271000
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_rounded,
        budget_adjusted,
        TRIM(location) AS clean_location,
        CONCAT_WS('/', dept_name, location) AS dept_name_location_key
    FROM cleaned
    WHERE budget_rounded BETWEEN 56 AND 67
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_adjusted,
        clean_location,
        dept_name_location_key,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget_adjusted, id) AS budget_adjusted_bucket,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget_rounded DESC, id) AS budget_rounded_bucket,
        SUM(budget_adjusted) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_adjusted,
        MAX(budget) OVER (PARTITION BY dept_name, location) AS dept_name_max_budget
    FROM valid
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(budget) OVER w_ordered AS last7_sum_budget,
        AVG(budget) OVER w_ordered AS last7_avg_budget,
        MAX(budget) OVER w_ordered AS last7_max_budget,
        MIN(budget) OVER w_ordered AS last7_min_budget
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY location, dept_name ORDER BY budget_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    dept_name,
    location,
    STDDEV_SAMP(budget) AS budget_stddev,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count,
    SIZE(COLLECT_SET(location)) AS num_locations
FROM enriched
GROUP BY dept_name, location;
