SELECT 'subscription' AS origin, start_date
FROM subscriptions
WHERE monthly_fee <= 160
UNION
SELECT 'store_sale' AS origin, sale_date
FROM store_sales
WHERE units <= 39
ORDER BY start_date
