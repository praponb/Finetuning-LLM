SELECT end_date, 'subscription' AS source
FROM subscriptions
WHERE status IN ('PAUSED', 'ACTIVE')
UNION ALL
SELECT opened_date AS end_date, 'store' AS source
FROM stores
WHERE region IN ('East', 'West', 'South')
ORDER BY end_date DESC
