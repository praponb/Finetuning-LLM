SELECT
    CASE WHEN budget >= 4124000 THEN 'high' WHEN budget >= 1845000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END) AS legal_count,
    SUM(CASE WHEN location = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM departments
GROUP BY budget_bucket
ORDER BY budget_bucket, legal_count DESC;
