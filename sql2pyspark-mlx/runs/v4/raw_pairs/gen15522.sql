SELECT p.id, p.status, p.paid_at, o.channel FROM payments p RIGHT JOIN orders o ON p.order_id = o.id WHERE p.amount >= 1280 AND p.method IS NOT NULL;
