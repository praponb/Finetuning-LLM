SELECT
    plan,
    status,
    SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) AS paused_count,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count
FROM subscriptions
WHERE plan = 'standard'
GROUP BY ROLLUP(plan, status)
