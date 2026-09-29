SELECT
    channel,
    CASE WHEN status IN ('SHIPPED', 'COMPLETED') THEN 'priority' ELSE 'other' END AS status_group,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM orders
WHERE discount >= 0.39
GROUP BY channel, status_group
ORDER BY channel
