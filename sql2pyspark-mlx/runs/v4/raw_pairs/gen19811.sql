SELECT p.id, p.amount FROM payments p WHERE p.method = 'bank_transfer' AND p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee < 24) ORDER BY amount DESC;
