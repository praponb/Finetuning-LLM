WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        MAX(budget) OVER (PARTITION BY location) AS location_max_budget
    FROM departments
    WHERE budget BETWEEN 2678000 AND 3439000
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        cumulative_budget - location_max_budget AS cumulative_budget_minus_location_max_budget
    FROM with_windows
    WHERE dept_name IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY cumulative_budget DESC) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
with_windows_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(location_max_budget, TRUE) OVER (ORDER BY cumulative_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location_max_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY cumulative_budget DESC, id) AS row_seq
    FROM latest
)
-- Window metrics over several different partitions
SELECT
    id,
    dept_name,
    location_max_budget,
    dept_name_length,
    cumulative_budget_minus_location_max_budget,
    latest_rank,
    row_seq,
    PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY cumulative_budget_minus_location_max_budget NULLS FIRST) AS cumulative_budget_minus_location_max_budget_percent_rank,
    ROW_NUMBER() OVER (ORDER BY dept_name_length DESC, id) AS event_seq,
    LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY cumulative_budget_minus_location_max_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name
FROM with_windows_departments
