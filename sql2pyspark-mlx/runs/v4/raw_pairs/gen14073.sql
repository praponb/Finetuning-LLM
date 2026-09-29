WITH recent_departments AS (
    SELECT *, LOWER(location) AS location_lower
    FROM departments
    WHERE (dept_name IN ('Marketing', 'Finance', 'Engineering') AND dept_name <> 'Sales') OR budget BETWEEN 2515000 AND 3817000
),
BudgetBucketSummary AS (
    SELECT
        CASE WHEN budget >= 3970000 THEN 'high' WHEN budget >= 1036000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        location,
        COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count,
        MAX(budget) AS max_budget
    FROM recent_departments
    GROUP BY CASE WHEN budget >= 3970000 THEN 'high' WHEN budget >= 1036000 THEN 'mid' ELSE 'low' END, location
)
SELECT budget_bucket, location, max_budget
FROM BudgetBucketSummary
ORDER BY max_budget DESC
