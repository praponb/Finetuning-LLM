SELECT o.channel, ROUND((100 * SUM(CASE WHEN s.status = 'DELIVERED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_delivered
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
GROUP BY o.channel
ORDER BY o.channel;
