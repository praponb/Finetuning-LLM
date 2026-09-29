WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(dept_name, 1, 3) dept_name_prefix,
        TRIM(dept_name) clean_dept_name,
        COALESCE(dept_name, 'N/A') resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND LENGTH(ch.full_name) >= 8
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_prefix,
        clean_dept_name,
        resolved_dept_name,
        INITCAP(TRIM(dept_name)) dept_name_title
    FROM eligible
    WHERE dept_name IN ('Legal', 'Finance', 'Sales')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT location
    FROM filtered
    EXCEPT ALL
    SELECT location
    FROM departments
    WHERE budget BETWEEN 2814000 AND 3341000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.location = k.location
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 3816000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    COUNT_IF(location = 'Singapore') AS singapore_events,
    AVG(budget) AS avg_budget,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    MAX(budget) FILTER (WHERE dept_name = 'Sales') AS sales_max_budget,
    SUM(budget) AS total_budget
FROM kept_rows
GROUP BY CUBE (CASE WHEN budget >= 3816000 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT_IF(location = 'Singapore') >= 1
ORDER BY avg_budget DESC NULLS FIRST
