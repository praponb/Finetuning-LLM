WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE customer_id = 6
),
PlanSummary AS (
    SELECT plan, SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee
    FROM recent_subscriptions
    GROUP BY plan
)
SELECT plan, enterprise_monthly_fee
FROM PlanSummary
WHERE enterprise_monthly_fee < 49
ORDER BY plan;
