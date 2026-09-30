SELECT p.id, p.paid_at, p.amount
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE discount BETWEEN 0.36 AND 0.39)
ORDER BY amount DESC;
