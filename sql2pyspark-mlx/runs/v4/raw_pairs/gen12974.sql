WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE monthly_fee < 170 AND status = 'EXPIRED' AND monthly_fee >= 150
),
subscriptions_stats AS (
    SELECT CASE WHEN monthly_fee >= 218 THEN 'high' WHEN monthly_fee >= 37 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, YEAR(start_date) AS start_year, ROUND((100 * SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paused
    FROM recent_subscriptions
    GROUP BY CASE WHEN monthly_fee >= 218 THEN 'high' WHEN monthly_fee >= 37 THEN 'mid' ELSE 'low' END, YEAR(start_date)
)
SELECT monthly_fee_bucket, start_year, pct_paused
FROM subscriptions_stats
WHERE pct_paused > 77
