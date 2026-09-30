SELECT p.id, p.amount, p.status, p.paid_at FROM payments p WHERE TO_DATE(p.paid_at) >= '2024-02-01' AND p.order_id NOT IN (SELECT id FROM orders WHERE customer_id = 27)
