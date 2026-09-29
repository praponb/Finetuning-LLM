WITH base AS (
    SELECT *, COALESCE(dept_name, 'Unknown') AS dept_name_filled
    FROM departments
    WHERE location IS NULL AND location = 'Chiang Mai'
),
budget_bucket_summary AS (
    SELECT CASE WHEN budget >= 3076000 THEN 'high' WHEN budget >= 1772000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
    FROM base
    GROUP BY CASE WHEN budget >= 3076000 THEN 'high' WHEN budget >= 1772000 THEN 'mid' ELSE 'low' END, location
)
SELECT *
FROM budget_bucket_summary
ORDER BY budget_bucket
