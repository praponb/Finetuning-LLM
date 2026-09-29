SELECT o.id, o.discount
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.status IN ('FAILED', 'REFUNDED'))
ORDER BY o.discount, id
LIMIT 10;
