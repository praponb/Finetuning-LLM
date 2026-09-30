SELECT o.status, COUNT(DISTINCT p.amount) AS unique_statuss, MAX(p.paid_at) AS latest_paid_at
FROM payments p
JOIN orders o ON p.order_id = o.id
GROUP BY o.status
