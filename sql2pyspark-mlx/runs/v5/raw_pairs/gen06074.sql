WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN budget >= 3480000 THEN 'High' WHEN budget >= 831000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM departments
    WHERE budget > 3759000
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        RANK() OVER (ORDER BY budget ASC NULLS FIRST) AS budget_rank
    FROM base
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(dept_name, 1, 4) AS dept_name_prefix_2
    FROM enriched
    WHERE location IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget_level,
        s.budget_share,
        s.budget_rank,
        top1.employee_level
    FROM valid s
    JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    employee_level,
    CASE WHEN budget_share >= 0.6 THEN 'HIGH' ELSE 'LOW' END AS budget_share_tier,
    CASE WHEN budget_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS budget_rank_tier,
    MAX(budget_rank) FILTER (WHERE employee_level = 'L1') AS l1_max_budget_rank,
    MAX(budget_share) AS max_budget_share,
    SUM(CASE WHEN employee_level = 'L1' THEN budget_rank ELSE 0 END) AS l1_budget_rank,
    COUNT(CASE WHEN employee_level = 'L3' THEN 1 END) AS l3_count,
    COUNT(CASE WHEN employee_level = 'L4' THEN 1 END) AS l4_count,
    COUNT(CASE WHEN employee_level = 'L3' THEN 1 END) AS l3_count_2,
    PERCENT_RANK() OVER (PARTITION BY employee_level ORDER BY MAX(budget_share) DESC) AS max_budget_share_percent_rank
FROM latest_match
GROUP BY 1, 2, 3
ORDER BY l3_count DESC;
