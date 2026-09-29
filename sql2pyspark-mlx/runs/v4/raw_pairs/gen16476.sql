SELECT budget_bucket, budget_count
FROM (
    SELECT CASE WHEN budget >= 3087000 THEN 'high' WHEN budget >= 1166000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(budget) AS budget_count
    FROM departments
    GROUP BY CASE WHEN budget >= 3087000 THEN 'high' WHEN budget >= 1166000 THEN 'mid' ELSE 'low' END
) AS x
WHERE budget_count >= 7
ORDER BY budget_bucket
LIMIT 20;
