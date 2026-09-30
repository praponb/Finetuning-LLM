SELECT CASE WHEN plan IN ('basic', 'enterprise') THEN 'group_a' ELSE 'other' END AS plan_group, COUNT(*) AS subscription_count
FROM subscriptions
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;
