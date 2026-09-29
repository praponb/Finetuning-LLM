WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(dept_name) AS dept_name_lower
    FROM departments
    WHERE dept_name = 'Finance'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(budget) OVER w_recent AS last4_avg_budget,
        COUNT(budget) OVER w_recent AS last4_count_budget,
        MIN(budget) OVER w_recent AS last4_min_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM cleaned
    WHERE budget BETWEEN 1887000 AND 3118000
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS rn
    FROM with_windows
    QUALIFY rn = 1
),
deduped_departments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY rn, id) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        budget,
        last4_avg_budget,
        last4_min_budget,
        budget_share,
        rn,
        COALESCE(dept_name, 'Unknown') AS dept_name_filled,
        budget_share + rn AS budget_share_plus_rn
    FROM deduped_departments
    WHERE last4_avg_budget < 3426000
)
-- Multi-level aggregation
SELECT
    CASE WHEN budget_share >= 0.69 THEN 'LARGE' ELSE 'SMALL' END AS budget_share_tier,
    dept_name,
    ROUND(SUM(budget_share) / NULLIF(COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END), 0), 2) AS budget_share_per_legal,
    MIN(last4_min_budget) AS min_last4_min_budget,
    ROUND(SUM(budget_share) / NULLIF(COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END), 0), 2) AS budget_share_per_sales,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM filtered
GROUP BY 1, 2
