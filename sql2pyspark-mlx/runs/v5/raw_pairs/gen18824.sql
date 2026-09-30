SELECT
    CASE WHEN monthly_fee >= 263 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT_IF(plan = 'standard') AS standard_events
FROM subscriptions
GROUP BY CASE WHEN monthly_fee >= 263 THEN 'HIGH' ELSE 'LOW' END
ORDER BY monthly_fee_tier DESC
