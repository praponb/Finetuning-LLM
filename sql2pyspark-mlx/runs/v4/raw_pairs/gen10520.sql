SELECT p.id, p.paid_at, p.amount FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 21 AND 25)
