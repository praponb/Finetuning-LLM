SELECT plan, COUNT(*) subscription_count
FROM subscriptions
GROUP BY plan
ORDER BY subscription_count DESC, plan
LIMIT 3;
