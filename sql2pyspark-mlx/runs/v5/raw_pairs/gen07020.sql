SELECT o.store_id, MIN(p.paid_at) AS min_paid_at
FROM payments p
RIGHT JOIN orders o ON p.order_id = o.id
GROUP BY o.store_id
ORDER BY min_paid_at DESC;
