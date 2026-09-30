SELECT budget_bucket, new_york_count
FROM (
    SELECT CASE WHEN budget >= 3973000 THEN 'high' WHEN budget >= 1158000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END) AS new_york_count
    FROM departments
    WHERE dept_name IN ('Marketing', 'Finance', 'Engineering')
    GROUP BY CASE WHEN budget >= 3973000 THEN 'high' WHEN budget >= 1158000 THEN 'mid' ELSE 'low' END
) AS t
WHERE new_york_count < 24
ORDER BY new_york_count DESC, budget_bucket
LIMIT 3
