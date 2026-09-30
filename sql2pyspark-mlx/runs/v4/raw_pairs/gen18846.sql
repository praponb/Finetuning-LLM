WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(location, 'unassigned') location_filled,
        CONCAT(dept_name, ' ', location) dept_name_location
    FROM departments
    WHERE dept_name IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_filled,
        dept_name_location,
        AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) budget_avg_6,
        LEAD(budget, 2) OVER (PARTITION BY dept_name ORDER BY budget, id) next_budget,
        LAG(budget, 1) OVER w_recent prev_budget
    FROM base
    WINDOW w_recent AS (PARTITION BY dept_name, location ORDER BY budget, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY prev_budget DESC NULLS LAST, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
)
-- Grouped metrics
SELECT
    CASE WHEN prev_budget >= 3523000 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_budget_tier,
    location,
    COUNT(*) AS event_count,
    AVG(budget) FILTER (WHERE location = 'London') AS london_avg_budget,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM deduped
GROUP BY ROLLUP (CASE WHEN prev_budget >= 3523000 THEN 'PREMIUM' ELSE 'STANDARD' END, location)
ORDER BY london_avg_budget DESC NULLS FIRST;
