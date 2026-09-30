SELECT
    channel,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    MAX(order_date) AS latest_order_date,
    COUNT(DISTINCT discount) AS unique_stores
FROM orders
GROUP BY channel
ORDER BY unique_stores, web_count DESC, channel
LIMIT 10
