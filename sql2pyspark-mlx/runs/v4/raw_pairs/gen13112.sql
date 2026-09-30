SELECT
    CASE WHEN budget >= 3181000 THEN 'high' WHEN budget >= 1826000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    COUNT(*) AS total_count,
    AVG(budget) AS avg_budget,
    ROUND((100 * SUM(CASE WHEN location = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin
FROM departments
WHERE NOT (dept_name IS NOT NULL AND budget <= 2258000)
GROUP BY budget_bucket
HAVING pct_berlin > 69
