SELECT p.id, p.order_id
FROM payments p
WHERE HOUR(p.paid_at) BETWEEN 6 AND 11 AND p.order_id NOT IN (SELECT id FROM orders WHERE status <> 'RETURNED')
ORDER BY order_id DESC
