SELECT o.channel, o.status, MIN(p.paid_at) AS min_paid_at
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY o.channel, o.status;
