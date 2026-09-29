WITH filtered_subscriptions AS (
    SELECT *, UPPER(status) AS status_upper
    FROM subscriptions
    WHERE monthly_fee > 140 AND end_date >= '2025-09-03'
),
subscriptions_stats AS (
    SELECT CASE WHEN monthly_fee >= 268 THEN 'high' WHEN monthly_fee >= 96 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count
    FROM filtered_subscriptions
    GROUP BY CASE WHEN monthly_fee >= 268 THEN 'high' WHEN monthly_fee >= 96 THEN 'mid' ELSE 'low' END
)
SELECT monthly_fee_bucket, enterprise_monthly_fee
FROM subscriptions_stats
WHERE monthly_fee_count > 8;
