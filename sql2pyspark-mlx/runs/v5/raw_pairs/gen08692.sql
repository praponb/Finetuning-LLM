SELECT o.store_id, MAX(p.amount) - MIN(p.amount) AS amount_range
FROM payments p
LEFT OUTER JOIN orders o ON o.id = p.order_id
WHERE o.status = 'RETURNED'
GROUP BY o.store_id
ORDER BY amount_range DESC;
