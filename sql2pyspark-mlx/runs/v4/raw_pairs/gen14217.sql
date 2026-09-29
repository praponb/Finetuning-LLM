SELECT p.id, p.paid_at
FROM payments p
WHERE TO_DATE(p.paid_at) < '2024-02-16' AND p.order_id IN (SELECT id FROM orders WHERE status <> 'SHIPPED')
ORDER BY paid_at
