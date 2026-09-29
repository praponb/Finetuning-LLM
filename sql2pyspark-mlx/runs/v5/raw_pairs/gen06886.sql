WITH filtered_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE customer_id IN (9, 16, 22) OR customer_id = 24
),
subscriptions_stats AS (
    SELECT CASE WHEN monthly_fee >= 270 THEN 'high' WHEN monthly_fee >= 79 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM filtered_subscriptions
    GROUP BY CASE WHEN monthly_fee >= 270 THEN 'high' WHEN monthly_fee >= 79 THEN 'mid' ELSE 'low' END, status
)
SELECT *
FROM subscriptions_stats
ORDER BY stddev_monthly_fee DESC
