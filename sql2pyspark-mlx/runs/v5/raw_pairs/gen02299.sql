SELECT location, budget_bucket, stddev_budget
FROM (
    SELECT location, CASE WHEN budget >= 3624000 THEN 'high' WHEN budget >= 1251000 THEN 'mid' ELSE 'low' END budget_bucket, ROUND(STDDEV(budget), 2) stddev_budget
    FROM departments
    GROUP BY location, CASE WHEN budget >= 3624000 THEN 'high' WHEN budget >= 1251000 THEN 'mid' ELSE 'low' END
) t
WHERE stddev_budget > 32
ORDER BY budget_bucket, location;
