WITH kept_keys AS (
    -- Set difference on keys
    SELECT location, id
    FROM departments
    EXCEPT
    SELECT location, id
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'Marketing', 'Legal')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.location = k.location AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS budget_dense_rank,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 4) AS budget_share,
        RANK() OVER (ORDER BY budget NULLS FIRST) AS budget_rank,
        MAX(budget) OVER (PARTITION BY location) AS location_max_budget,
        DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC) AS budget_dense_rank_2
    FROM verified
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        budget,
        budget_dense_rank,
        budget_share,
        budget_rank,
        LEAST(location_max_budget, budget_rank) AS least_location_max_budget_budget_rank,
        COALESCE(location, 'NONE') AS location_filled
    FROM with_windows
    WHERE location IS NOT NULL
      AND budget_rank <= 6
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM cleaned
    WHERE dept_name IS NOT NULL
      AND least_location_max_budget_budget_rank >= 43
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN budget_filled >= 70 THEN 'LARGE' ELSE 'SMALL' END AS budget_filled_tier,
        SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
        COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count,
        ROUND(SUM(budget_filled) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS budget_filled_per_engineering,
        SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list_2
    FROM base
    GROUP BY 1
)
SELECT
    budget_filled_tier,
    marketing_count,
    dept_name_list_2
FROM grouped
