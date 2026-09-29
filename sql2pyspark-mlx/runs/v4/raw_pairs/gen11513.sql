SELECT
    status,
    CASE WHEN monthly_fee >= 250 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    COUNT_IF(plan = 'standard') AS standard_events,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY status, monthly_fee_tier
HAVING COUNT_IF(plan = 'standard') >= 1
ORDER BY avg_monthly_fee DESC NULLS FIRST
