SELECT
    p.method,
    o.channel,
    o.status,
    s.region,
    s.store_name,
    SUBSTRING(o.channel, 1, 3) AS channel_prefix
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE HOUR(p.paid_at) BETWEEN 11 AND 17
ORDER BY channel_prefix DESC
