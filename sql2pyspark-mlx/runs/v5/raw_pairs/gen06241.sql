SELECT sale_date AS event_date
FROM store_sales
UNION ALL
SELECT start_date AS event_date
FROM subscriptions
WHERE status = 'PAUSED'
