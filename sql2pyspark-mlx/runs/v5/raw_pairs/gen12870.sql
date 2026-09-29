SELECT *
FROM (
    SELECT CASE WHEN budget >= 3041000 THEN 'high' WHEN budget >= 1936000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_locations
    FROM departments
    GROUP BY CASE WHEN budget >= 3041000 THEN 'high' WHEN budget >= 1936000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE unique_locations < 5
ORDER BY 1 DESC
