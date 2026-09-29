WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND LENGTH(ch.full_name) < 6
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN budget >= 4393000 THEN 'High' WHEN budget >= 764000 THEN 'Medium' ELSE 'Low' END AS budget_tier,
        COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM not_flagged
    WHERE location IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN budget >= 3065000 THEN 'High' WHEN budget >= 1996000 THEN 'Medium' ELSE 'Low' END AS budget_tier_2
    FROM cleaned
    WHERE dept_name IS DISTINCT FROM 'Sales'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget_tier,
        budget_tier_2,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket
    FROM base
    WHERE budget > 1645000
)
-- Grouped metrics
SELECT
    dept_name,
    ROUND(AVG(budget_bucket), 2) AS avg_budget_bucket,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY dept_name;
