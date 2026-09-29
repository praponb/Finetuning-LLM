SELECT o.status, MIN(p.amount) min_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY o.status
HAVING MIN(p.amount) >= 1500;
