SELECT o.status, COUNT(p.id) AS payment_count, MIN(p.paid_at) AS min_paid_at
FROM orders o
INNER JOIN payments p ON o.id = p.order_id
GROUP BY o.status;
