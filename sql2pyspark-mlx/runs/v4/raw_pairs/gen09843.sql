WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN dept_name IN ('Legal', 'Marketing') THEN 'Group A' ELSE 'Other' END dept_name_group
    FROM departments
    WHERE budget BETWEEN 2278000 AND 3555000
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.is_remote = FALSE
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id ASC) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM deduped
    ) x
    WHERE rank_in_group = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        dept_name_group,
        rank_in_group,
        GREATEST(budget, rank_in_group) greatest_budget_rank_in_group,
        SUBSTRING(location, 1, 2) location_prefix
    FROM ranked
    WHERE dept_name IS NOT NULL
)
-- Grouped metrics
SELECT
    dept_name,
    CASE WHEN greatest_budget_rank_in_group >= 83 THEN 'HIGH' ELSE 'LOW' END AS greatest_budget_rank_in_group_tier,
    SUM(greatest_budget_rank_in_group) FILTER (WHERE location = 'London') AS london_total_greatest_budget_rank_in_group,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM valid
GROUP BY GROUPING SETS ((dept_name, CASE WHEN greatest_budget_rank_in_group >= 83 THEN 'HIGH' ELSE 'LOW' END), (dept_name), ())
ORDER BY dept_name DESC
