SELECT p.id, p.amount FROM payments p WHERE p.method = 'bank_transfer' AND p.order_id NOT IN (SELECT id FROM orders WHERE customer_id = 32) ORDER BY p.amount DESC, p.id DESC
